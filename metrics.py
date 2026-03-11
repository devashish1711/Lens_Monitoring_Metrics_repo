# metrices.py

from __future__ import annotations

import json
import re
import argparse
import base64
import json
import subprocess
import os
import sys
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import time
from datetime import datetime, timedelta, timezone

from typing import Dict, Any

try:
    from google.cloud import monitoring_v3
except Exception:
    monitoring_v3 = None

try:
    from google.cloud import logging_v2
except Exception:
    logging_v2 = None

try:
    from google.protobuf import duration_pb2
except Exception:
    duration_pb2 = None

try:
    from google.oauth2 import service_account
    import google.auth
    from google.auth.credentials import Credentials
except Exception:
    service_account = None
    google = None
    Credentials = Any

try:
    from google.cloud import resourcemanager_v3
except Exception:
    resourcemanager_v3 = None

try:
    from google.cloud import compute_v1
except Exception:
    compute_v1 = None


try:
    from google.cloud import container_v1
except Exception:
    container_v1 = None

try:
    from googleapiclient.discovery import build as gapi_build
except Exception:
    gapi_build = None


@dataclass
class GcpConnectionInput:
    """
    Represents what the end-user can provide as "credentials".

    Equivalent of AWS access-key/secret is NOT a user password in GCP.
    For automation, the standard is a Service Account JSON key (or impersonation/OAuth).

    Supported inputs:
      - service_account_file: path to JSON key file
      - service_account_json: raw JSON string
      - service_account_b64: base64(JSON string) (common for UI forms/env vars)
      - project_id: single project scope (optional; else discover from credentials access)
    """

    service_account_file: Optional[str] = None
    service_account_json: Optional[str] = None
    service_account_b64: Optional[str] = None
    project_id: Optional[str] = None


@dataclass
class NormalizedResource:
    """
    Common shape for all resources in your UI.

    This is how you get consistency across VM vs GKE vs SQL vs Networking.
    """

    service: str  # "vm" | "gke" | "database" | "networking"
    resource_type: str  # e.g., "compute_instance", "gke_cluster", "cloudsql_instance", "vpc_network"
    name: str  # display name
    id: Optional[str]  # provider id (where available)
    project_id: str
    location: Optional[str]  # zone/region/global
    status: Optional[str]  # RUNNING/STOPPED/...
    labels: Dict[str, str]
    raw: Dict[str, Any]  # raw minimal info, not the full API object (keep payload sane)


@dataclass
class InventoryResult:
    """
    Output contract for inventory for a single project.
    """

    project_id: str
    services: Dict[str, List[NormalizedResource]]
    errors: List[str]


VM_MONITORING_CATALOG = {
    "CPU": {
        "label": "CPU Metrics",
        "metrics": {
            "cpu_utilization": {
                "label": "CPU Utilization",
                "description": "Percentage of CPU being used.",
                "unit": "%",
                "threshold_type": "percentage",
                "default_warning": 70,
                "default_critical": 85,
                "duration_options": [60, 300, 600],
                "gcp_metric": 'metric.type="compute.googleapis.com/instance/cpu/utilization"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "percent_to_fraction",
            },
            "cpu_load_1m": {
                "label": "CPU Load Average (1m)",
                "description": "Short-term system load. (Requires Ops Agent)",
                "unit": "load",
                "threshold_type": "number",
                "default_warning": 2.0,
                "default_critical": 4.0,
                "duration_options": [60, 300],
                "gcp_metric": 'metric.type="agent.googleapis.com/cpu/load_1m"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
            "cpu_load_5m": {
                "label": "CPU Load Average (5m)",
                "description": "Medium-term system load. (Requires Ops Agent)",
                "unit": "load",
                "threshold_type": "number",
                "default_warning": 1.8,
                "default_critical": 3.5,
                "duration_options": [300, 600],
                "gcp_metric": 'metric.type="agent.googleapis.com/cpu/load_5m"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
            "cpu_load_15m": {
                "label": "CPU Load Average (15m)",
                "description": "Long-term system load. (Requires Ops Agent)",
                "unit": "load",
                "threshold_type": "number",
                "default_warning": 1.5,
                "default_critical": 3.0,
                "duration_options": [300, 600, 900],
                "gcp_metric": 'metric.type="agent.googleapis.com/cpu/load_15m"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
            "custom_cpu": {
                "label": "Add Custom CPU Metric",
                "description": "Create your own CPU monitoring rule",
                "unit": "",
                "threshold_type": "custom",
                "default_warning": None,
                "default_critical": None,
                "duration_options": [],
                "gcp_metric": "CUSTOM",
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
        },
    },
    "Memory": {
        "label": "Memory Metrics",
        "metrics": {
            "memory_utilization": {
                "label": "Memory Usage",
                "description": "Percentage of RAM being used. (Requires Ops Agent)",
                "unit": "%",
                "threshold_type": "percentage",
                "default_warning": 75,
                "default_critical": 90,
                "duration_options": [60, 300, 600],
                "gcp_metric": 'metric.type="agent.googleapis.com/memory/percent_used" AND metric.labels.state="used"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
            "available_memory": {
                "label": "Available Memory",
                "description": "Free/available RAM remaining. Low available memory indicates pressure. (Requires Ops Agent)",
                "unit": "GB",
                "threshold_type": "number",
                "default_warning": 2.0,
                "default_critical": 1.0,
                "duration_options": [60, 300, 600],
                "gcp_metric": 'metric.type="agent.googleapis.com/memory/bytes_used" AND metric.labels.state="free"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "gb_to_bytes",  # <-- Tells the CLI to do the math!
            },
            "swap_utilization": {
                "label": "Swap Memory Usage",
                "description": "Percentage of swap space being used. High swap indicates severe RAM pressure. (Requires Ops Agent)",
                "unit": "%",
                "threshold_type": "percentage",
                "default_warning": 40,
                "default_critical": 80,
                "duration_options": [300, 600],
                "gcp_metric": 'metric.type="agent.googleapis.com/swap/percent_used" AND metric.labels.state="used"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
            "custom_memory": {
                "label": "Add Custom Memory Metric",
                "description": "Create your own memory monitoring rule",
                "unit": "",
                "threshold_type": "custom",
                "default_warning": None,
                "default_critical": None,
                "duration_options": [],
                "gcp_metric": "CUSTOM",
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
        },
    },
    "Disk": {
        "label": "Disk Metrics",
        "metrics": {
            "disk_usage": {
                "label": "Disk Usage",
                "description": "Percentage of disk space used. (Requires Ops Agent)",
                "unit": "%",
                "threshold_type": "percentage",
                "default_warning": 80,
                "default_critical": 90,
                "duration_options": [300, 600, 900],
                "gcp_metric": 'metric.type="agent.googleapis.com/disk/percent_used" AND metric.labels.state="used"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
            "disk_read_iops": {
                "label": "Disk Read IOPS",
                "description": "Number of disk read operations per second.",
                "unit": "ops/s",
                "threshold_type": "number",
                "default_warning": 500,
                "default_critical": 1000,
                "duration_options": [60, 300, 600],
                "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/read_ops_count"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_RATE",
                "transform": "identity",
            },
            "disk_write_iops": {
                "label": "Disk Write IOPS",
                "description": "Number of disk write operations per second.",
                "unit": "ops/s",
                "threshold_type": "number",
                "default_warning": 500,
                "default_critical": 1000,
                "duration_options": [60, 300, 600],
                "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/write_ops_count"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_RATE",
                "transform": "identity",
            },
            "disk_read_throughput": {
                "label": "Disk Read Throughput",
                "description": "Amount of data read per second.",
                "unit": "MB/s",
                "threshold_type": "number",
                "default_warning": 50,
                "default_critical": 100,
                "duration_options": [60, 300, 600],
                "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/read_bytes_count"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_RATE",
                "transform": "mb_to_bytes",
            },
            "disk_write_throughput": {
                "label": "Disk Write Throughput",
                "description": "Amount of data written per second.",
                "unit": "MB/s",
                "threshold_type": "number",
                "default_warning": 50,
                "default_critical": 100,
                "duration_options": [60, 300, 600],
                "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/write_bytes_count"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_RATE",
                "transform": "mb_to_bytes",
            },
            "disk_read_latency": {
                "label": "Disk Read Latency",
                "description": "Time taken for disk read operations.",
                "unit": "ms",
                "threshold_type": "number",
                "default_warning": 20,
                "default_critical": 50,
                "duration_options": [60, 300],
                "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/read_latencies"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_PERCENTILE_99",
                "transform": "identity",
            },
            "disk_write_latency": {
                "label": "Disk Write Latency",
                "description": "Time taken for disk write operations.",
                "unit": "ms",
                "threshold_type": "number",
                "default_warning": 20,
                "default_critical": 50,
                "duration_options": [60, 300],
                "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/write_latencies"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_PERCENTILE_99",
                "transform": "identity",
            },
            "custom_disk": {
                "label": "Add Custom Disk Metric",
                "description": "Create your own disk monitoring rule",
                "unit": "",
                "threshold_type": "custom",
                "default_warning": None,
                "default_critical": None,
                "duration_options": [],
                "gcp_metric": "CUSTOM",
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
        },
    },
    "Network": {
        "label": "Network Metrics",
        "metrics": {
            "network_in": {
                "label": "Network Incoming Traffic",
                "description": "Data received by the VM.",
                "unit": "MB/s",
                "threshold_type": "number",
                "default_warning": 10,
                "default_critical": 50,
                "duration_options": [60, 300, 600],
                "gcp_metric": 'metric.type="compute.googleapis.com/instance/network/received_bytes_count"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_RATE",
                "transform": "mb_to_bytes",
            },
            "network_out": {
                "label": "Network Outgoing Traffic",
                "description": "Data sent by the VM.",
                "unit": "MB/s",
                "threshold_type": "number",
                "default_warning": 10,
                "default_critical": 50,
                "duration_options": [60, 300, 600],
                "gcp_metric": 'metric.type="compute.googleapis.com/instance/network/sent_bytes_count"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_RATE",
                "transform": "mb_to_bytes",
            },
            "tcp_connections": {
                "label": "Active TCP Connections",
                "description": "Current number of TCP connections. (Requires Ops Agent)",
                "unit": "connections",
                "threshold_type": "number",
                "default_warning": 1000,
                "default_critical": 5000,
                "duration_options": [60, 300],
                "gcp_metric": 'metric.type="agent.googleapis.com/network/tcp_connections"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
            "network_errors": {
                "label": "Network Errors",
                "description": "Total interface errors on send/receive path. (Requires Ops Agent)",
                "unit": "errors/s",
                "threshold_type": "number",
                "default_warning": 10,
                "default_critical": 50,
                "duration_options": [60, 300],
                "gcp_metric": 'metric.type="agent.googleapis.com/network/errors"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_RATE",
                "transform": "identity",
            },
            "custom_network": {
                "label": "Add Custom Network Metric",
                "description": "Create your own network monitoring rule",
                "unit": "",
                "threshold_type": "custom",
                "default_warning": None,
                "default_critical": None,
                "duration_options": [],
                "gcp_metric": "CUSTOM",
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
        },
    },
    "Processes": {
        "label": "Process Metrics",
        "metrics": {
            "total_proc": {
                "label": "Total Process Count",
                "description": "Total number of running processes. (Requires Ops Agent)",
                "unit": "count",
                "threshold_type": "number",
                "default_warning": 300,
                "default_critical": 500,
                "duration_options": [60, 300],
                "gcp_metric": 'metric.type="agent.googleapis.com/processes/count"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
            "zombie_proc": {
                "label": "Zombie Process Count",
                "description": "Number of zombie processes. (Requires Ops Agent)",
                "unit": "count",
                "threshold_type": "number",
                "default_warning": 5,
                "default_critical": 20,
                "duration_options": [60, 300],
                "gcp_metric": 'metric.type="agent.googleapis.com/processes/count_by_state" AND metric.labels.state="zombie"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
            "proc_rss": {
                "label": "Process Memory (RSS)",
                "description": "Resident memory used by processes. (Requires Ops Agent)",
                "unit": "MB",
                "threshold_type": "number",
                "default_warning": 500,
                "default_critical": 1024,
                "duration_options": [60, 300],
                "gcp_metric": 'metric.type="agent.googleapis.com/processes/rss_usage"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "mb_to_bytes",
            },
            "proc_cpu": {
                "label": "Process CPU Usage",
                "description": "CPU used by processes. (Requires Ops Agent)",
                "unit": "seconds/s",
                "threshold_type": "number",
                "default_warning": 0.8,
                "default_critical": 0.95,
                "duration_options": [60, 300],
                "gcp_metric": 'metric.type="agent.googleapis.com/processes/cpu_time"',
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_RATE",
                "transform": "identity",
            },
            "custom_process": {
                "label": "Add Custom Process Metric",
                "description": "Create your own process monitoring rule",
                "unit": "",
                "threshold_type": "custom",
                "default_warning": None,
                "default_critical": None,
                "duration_options": [],
                "gcp_metric": "CUSTOM",
                "resource_type": 'resource.type="gce_instance"',
                "aligner": "ALIGN_MEAN",
                "transform": "identity",
            },
        },
    },
}


class NetworkingCatalog:
    @staticmethod
    def get_tabs(resource_type):
        tab_map = {
            "vpc": [
                "Overview",
                "Subnets",
                "Firewall Rules",
                "Routes",
                "Connectivity",
                "Alerts",
            ],
            "subnet": [
                "Overview",
                "Configuration",
                "Metrics",
                "Alerts",
                "Delete Subnet",
            ],
            "firewall": [
                "Overview",
                "Configuration",
                "Traffic Metrics",
                "Security Insights",
                "Alerts",
            ],
            "route": ["Overview", "Configuration"],
            "load_balancer": ["Overview", "Configuration", "Metrics", "Logs", "Alerts"],
            "router": ["Overview", "Configuration", "Metrics", "Alerts"],
            "nat": ["Overview", "Configuration", "Metrics", "Logs", "Alerts"],
        }
        return tab_map.get(resource_type, ["Overview"])


class AuthManager:
    """
    Handles conversion from user-provided credential inputs into a Credentials object.
    """

    DEFAULT_SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",
    ]

    @staticmethod
    def load_credentials(conn: GcpConnectionInput) -> Tuple[Credentials, Optional[str]]:
        if service_account is None:
            raise RuntimeError("Missing google-auth. Install: pip install google-auth")

        project_hint = conn.project_id

        # 1) STRICT: Exact JSON file path provided by user
        if conn.service_account_file:
            if not os.path.isfile(conn.service_account_file):
                raise FileNotFoundError(
                    f"Exact JSON file not found at: {conn.service_account_file}"
                )

            creds = service_account.Credentials.from_service_account_file(
                conn.service_account_file,
                scopes=AuthManager.DEFAULT_SCOPES,
            )
            return creds, project_hint

        # 2) From raw JSON string (For when your UI passes the uploaded file content directly)
        if conn.service_account_json:
            data = json.loads(conn.service_account_json)
            creds = service_account.Credentials.from_service_account_info(
                data,
                scopes=AuthManager.DEFAULT_SCOPES,
            )
            return creds, project_hint

        # 3) From base64 JSON string
        if conn.service_account_b64:
            decoded = base64.b64decode(conn.service_account_b64).decode("utf-8")
            data = json.loads(decoded)
            creds = service_account.Credentials.from_service_account_info(
                data,
                scopes=AuthManager.DEFAULT_SCOPES,
            )
            return creds, project_hint

        # 4) Fallback: ADC
        creds, inferred_project = google.auth.default(scopes=AuthManager.DEFAULT_SCOPES)
        return creds, project_hint or inferred_project


class ProjectDiscovery:
    """
    Decides which project(s) your app will fetch.

    Logic:
    - If user explicitly gave a project_id -> use it (fastest, simplest).
    - Else list projects visible to those credentials.
      This is the "console-like" experience (user selects project).
    """

    @staticmethod
    def list_accessible_projects(credentials: Credentials) -> List[str]:
        """
        Returns a list of project IDs accessible by credentials.

        In many orgs, the service account only has access to 1 project — still fine.
        """
        if resourcemanager_v3 is None:
            raise RuntimeError(
                "Missing google-cloud-resource-manager. Install: pip install google-cloud-resource-manager"
            )

        client = resourcemanager_v3.ProjectsClient(credentials=credentials)

        # NOTE:
        # - search_projects can list projects across the org if permitted.
        # - This is still "read-only inventory" behavior.
        project_ids: List[str] = []
        for p in client.search_projects(
            request=resourcemanager_v3.SearchProjectsRequest()
        ):
            # p.project_id is the human ID (what you want)
            if getattr(p, "project_id", None):
                project_ids.append(p.project_id)

        return sorted(set(project_ids))


class ComputeInventory:
    """
    Inventory for Compute Engine: instances (VMs).
    """

    @staticmethod
    def list_instances(
        credentials: Credentials, project_id: str
    ) -> List[NormalizedResource]:
        if compute_v1 is None:
            raise RuntimeError(
                "Missing google-cloud-compute. Install: pip install google-cloud-compute"
            )

        # Aggregated list gives you instances across all zones in one call pattern.
        client = compute_v1.InstancesClient(credentials=credentials)

        resources: List[NormalizedResource] = []

        req = compute_v1.AggregatedListInstancesRequest(project=project_id)
        for zone, scoped_list in client.aggregated_list(request=req):
            # zone looks like "zones/us-central1-a"
            if not scoped_list.instances:
                continue
            zone_name = zone.split("/")[-1] if zone else None

            for inst in scoped_list.instances:
                labels = dict(inst.labels or {})
                resources.append(
                    NormalizedResource(
                        service="vm",
                        resource_type="compute_instance",
                        name=inst.name or "",
                        id=str(inst.id) if inst.id is not None else None,
                        project_id=project_id,
                        location=zone_name,
                        status=inst.status,
                        labels=labels,
                        raw={
                            "machine_type": (
                                inst.machine_type.split("/")[-1]
                                if inst.machine_type
                                else None
                            ),
                            "network_interfaces": [
                                {
                                    "name": ni.name,
                                    "network": (
                                        ni.network.split("/")[-1]
                                        if ni.network
                                        else None
                                    ),
                                    "subnetwork": (
                                        ni.subnetwork.split("/")[-1]
                                        if ni.subnetwork
                                        else None
                                    ),
                                    "network_ip": ni.network_i_p,  # <-- Notice the _i_p here
                                }
                                for ni in (inst.network_interfaces or [])
                            ],
                        },
                    )
                )

        return resources


class GkeInventory:
    """
    Inventory for GKE clusters.
    """

    @staticmethod
    def list_clusters(
        credentials: Credentials, project_id: str
    ) -> List[NormalizedResource]:
        if container_v1 is None:
            raise RuntimeError(
                "Missing google-cloud-container. Install: pip install google-cloud-container"
            )

        client = container_v1.ClusterManagerClient(credentials=credentials)

        # "locations/-" = all locations
        parent = f"projects/{project_id}/locations/-"
        resp = client.list_clusters(parent=parent)

        resources: List[NormalizedResource] = []
        for c in resp.clusters or []:
            # c.location can be region/zone depending on cluster type
            labels = dict(c.resource_labels or {})
            resources.append(
                NormalizedResource(
                    service="gke",
                    resource_type="gke_cluster",
                    name=c.name or "",
                    id=c.self_link
                    or None,  # not always "id" field; self_link is unique-ish
                    project_id=project_id,
                    location=c.location or None,
                    status=str(c.status) if c.status is not None else None,
                    labels=labels,
                    raw={
                        "endpoint": c.endpoint,
                        "network": c.network,
                        "subnetwork": c.subnetwork,
                        "current_master_version": c.current_master_version,
                        "current_node_version": c.current_node_version,
                    },
                )
            )

        return resources


class CloudSqlInventory:
    """
    Inventory for Cloud SQL.
    Uses SQL Admin API (google-api-python-client discovery).
    """

    @staticmethod
    def list_instances(
        credentials: Credentials, project_id: str
    ) -> List[NormalizedResource]:
        if gapi_build is None:
            raise RuntimeError(
                "Missing google-api-python-client. Install: pip install google-api-python-client"
            )

        # SQL Admin API name: "sqladmin", version: "v1beta4"
        service = gapi_build(
            "sqladmin", "v1beta4", credentials=credentials, cache_discovery=False
        )

        req = service.instances().list(project=project_id)
        resp = req.execute()

        items = resp.get("items", []) if isinstance(resp, dict) else []
        resources: List[NormalizedResource] = []

        for it in items:
            settings = it.get("settings", {}) or {}
            labels = settings.get("userLabels", {}) or {}
            resources.append(
                NormalizedResource(
                    service="database",
                    resource_type="cloudsql_instance",
                    name=it.get("name", ""),
                    id=str(it.get("id")) if it.get("id") is not None else None,
                    project_id=project_id,
                    location=it.get("region"),
                    status=it.get("state"),
                    labels=labels,
                    raw={
                        "database_version": it.get("databaseVersion"),
                        "tier": settings.get("tier"),
                        "ip_addresses": it.get("ipAddresses", []),
                        "gce_zone": it.get("gceZone"),
                    },
                )
            )

        return resources


class NetworkingInventory:
    """
    Inventory for networking resources.
    Start small but useful:
      - VPC Networks
      - Subnets
      - Firewall rules
      - Routers (+ Cloud NAT configs inside routers)

    This matches your goal: "fetch console data like networking list".
    """

    @staticmethod
    def list_networks(
        credentials: Credentials, project_id: str
    ) -> List[NormalizedResource]:
        if compute_v1 is None:
            raise RuntimeError(
                "Missing google-cloud-compute. Install: pip install google-cloud-compute"
            )

        client = compute_v1.NetworksClient(credentials=credentials)
        resources: List[NormalizedResource] = []

        for net in client.list(project=project_id):
            resources.append(
                NormalizedResource(
                    service="networking",
                    resource_type="vpc_network",
                    name=net.name or "",
                    id=str(net.id) if net.id is not None else None,
                    project_id=project_id,
                    location="global",
                    status=None,
                    labels={},
                    raw={
                        "auto_create_subnetworks": net.auto_create_subnetworks,
                        "routing_config": {
                            "routing_mode": (
                                getattr(net.routing_config, "routing_mode", None)
                                if net.routing_config
                                else None
                            )
                        },
                    },
                )
            )
        return resources

    @staticmethod
    def list_routes(
        credentials: Credentials, project_id: str
    ) -> List[NormalizedResource]:
        if compute_v1 is None:
            raise RuntimeError("Missing google-cloud-compute.")
        client = compute_v1.RoutesClient(credentials=credentials)
        resources: List[NormalizedResource] = []
        for route in client.list(project=project_id):
            resources.append(
                NormalizedResource(
                    service="networking",
                    resource_type="route",
                    name=route.name or "",
                    id=str(route.id) if route.id else None,
                    project_id=project_id,
                    location="global",
                    status=None,
                    labels={},
                    raw={
                        "network": (
                            route.network.split("/")[-1] if route.network else None
                        ),
                        "dest_range": getattr(route, "dest_range", None),
                        "priority": getattr(route, "priority", None),
                        "tags": list(route.tags or []),
                        "next_hop": route.next_hop_gateway
                        or route.next_hop_ip
                        or "N/A",
                    },
                )
            )
        return resources

    @staticmethod
    def delete_subnet(
        credentials: Credentials, project_id: str, region: str, subnet_name: str
    ) -> Dict[str, Any]:
        if compute_v1 is None:
            raise RuntimeError(
                "Missing google-cloud-compute. Install: pip install google-cloud-compute"
            )

        client = compute_v1.SubnetworksClient(credentials=credentials)

        operation = client.delete(
            project=project_id,
            region=region,
            subnetwork=subnet_name,
        )

        return {
            "message": "Delete request submitted",
            "subnet": subnet_name,
            "region": region,
            "operation": getattr(operation, "name", None),
        }

    @staticmethod
    def list_subnets(
        credentials: Credentials, project_id: str
    ) -> List[NormalizedResource]:
        if compute_v1 is None:
            raise RuntimeError(
                "Missing google-cloud-compute. Install: pip install google-cloud-compute"
            )

        client = compute_v1.SubnetworksClient(credentials=credentials)
        resources: List[NormalizedResource] = []

        # Aggregated list across regions
        req = compute_v1.AggregatedListSubnetworksRequest(project=project_id)
        for region, scoped_list in client.aggregated_list(request=req):
            if not scoped_list.subnetworks:
                continue
            region_name = region.split("/")[-1] if region else None

            for sn in scoped_list.subnetworks:
                resources.append(
                    NormalizedResource(
                        service="networking",
                        resource_type="subnetwork",
                        name=sn.name or "",
                        id=str(sn.id) if sn.id is not None else None,
                        project_id=project_id,
                        location=region_name,
                        status=None,
                        labels={},
                        raw={
                            "network": (
                                sn.network.split("/")[-1] if sn.network else None
                            ),
                            "ip_cidr_range": sn.ip_cidr_range,
                            "private_ip_google_access": sn.private_ip_google_access,
                        },
                    )
                )
        return resources

    @staticmethod
    def list_firewall_rules(
        credentials: Credentials, project_id: str
    ) -> List[NormalizedResource]:
        if compute_v1 is None:
            raise RuntimeError(
                "Missing google-cloud-compute. Install: pip install google-cloud-compute"
            )

        client = compute_v1.FirewallsClient(credentials=credentials)
        resources: List[NormalizedResource] = []

        for fw in client.list(project=project_id):
            resources.append(
                NormalizedResource(
                    service="networking",
                    resource_type="firewall_rule",
                    name=fw.name or "",
                    id=str(fw.id) if fw.id is not None else None,
                    project_id=project_id,
                    location="global",
                    status=None,
                    labels={},
                    raw={
                        "direction": fw.direction,
                        "network": (fw.network.split("/")[-1] if fw.network else None),
                        "priority": fw.priority,
                        "source_ranges": list(fw.source_ranges or []),
                        "target_tags": list(fw.target_tags or []),
                        "allowed": [
                            {
                                "IPProtocol": (
                                    getattr(a, "IP_protocol", None)
                                    or getattr(a, "IPProtocol", None)
                                    or getattr(a, "i_p_protocol", None)
                                ),
                                "ports": list(a.ports or []),
                            }
                            for a in (fw.allowed or [])
                        ],
                        "denied": [
                            {
                                "IPProtocol": (
                                    getattr(d, "IP_protocol", None)
                                    or getattr(d, "IPProtocol", None)
                                    or getattr(d, "i_p_protocol", None)
                                ),
                                "ports": list(d.ports or []),
                            }
                            for d in (fw.denied or [])
                        ],
                    },
                )
            )
        return resources

    @staticmethod
    def list_routers_and_nats(
        credentials: Credentials, project_id: str
    ) -> List[NormalizedResource]:
        """
        Cloud NAT is configured on Cloud Router in GCP.
        So: list routers; inside router, list 'nats' configs.
        """
        if compute_v1 is None:
            raise RuntimeError(
                "Missing google-cloud-compute. Install: pip install google-cloud-compute"
            )

        client = compute_v1.RoutersClient(credentials=credentials)
        resources: List[NormalizedResource] = []

        # Aggregated list across regions
        req = compute_v1.AggregatedListRoutersRequest(project=project_id)
        for region, scoped_list in client.aggregated_list(request=req):
            if not scoped_list.routers:
                continue
            region_name = region.split("/")[-1] if region else None

            for r in scoped_list.routers:
                # router-level resource
                resources.append(
                    NormalizedResource(
                        service="networking",
                        resource_type="cloud_router",
                        name=r.name or "",
                        id=str(r.id) if r.id is not None else None,
                        project_id=project_id,
                        location=region_name,
                        status=None,
                        labels={},
                        raw={
                            "network": (
                                r.network.split("/")[-1] if r.network else None
                            ),
                            "bgp": {
                                "asn": getattr(r.bgp, "asn", None) if r.bgp else None,
                            },
                        },
                    )
                )

                # nat configs under router as separate resources (so UI can show them)
                for nat in r.nats or []:
                    resources.append(
                        NormalizedResource(
                            service="networking",
                            resource_type="cloud_nat",
                            name=nat.name or "",
                            id=None,  # NAT config doesn't have stable numeric ID the same way
                            project_id=project_id,
                            location=region_name,
                            status=None,
                            labels={},
                            raw={
                                "router": r.name,
                                "nat_ip_allocate_option": nat.nat_ip_allocate_option,
                                "source_subnetwork_ip_ranges_to_nat": nat.source_subnetwork_ip_ranges_to_nat,
                                "min_ports_per_vm": nat.min_ports_per_vm,
                                "udp_idle_timeout_sec": nat.udp_idle_timeout_sec,
                                "icmp_idle_timeout_sec": nat.icmp_idle_timeout_sec,
                            },
                        )
                    )

        return resources


class NetworkOrchestrator:
    @staticmethod
    def list_resources(resource_type, creds, project_id):
        if resource_type == "vpc":
            return NetworkingInventory.list_networks(creds, project_id)
        if resource_type == "subnet":
            return NetworkingInventory.list_subnets(creds, project_id)
        if resource_type == "firewall":
            return NetworkingInventory.list_firewall_rules(creds, project_id)
        if resource_type == "route":
            return NetworkingInventory.list_routes(creds, project_id)
        if resource_type in ["router", "nat"]:
            return NetworkingInventory.list_routers_and_nats(creds, project_id)
        return []


class ObservabilityCatalog:
    """
    Acts as a router for the UI.
    Given a selected resource, what tabs should the UI show?
    """

    @staticmethod
    def get_tabs(resource: NormalizedResource) -> List[str]:
        if resource.service == "vm":
            return [
                "CPU",
                "Memory",
                "Disk",
                "Network",
                "Processes",
                "Logs",
                "Events",
                "Alerts",
            ]
        elif resource.service == "gke":
            return ["Nodes", "Workloads", "CPU", "Memory", "Logs", "System Events"]
        elif resource.service == "database":
            return ["CPU", "Memory", "Connections", "Storage", "Logs"]
        elif resource.service == "networking":
            if resource.resource_type == "cloud_router":
                return ["BGP Sessions", "Logs"]
            elif resource.resource_type == "cloud_nat":
                return ["Port Usage", "Dropped Packets", "Logs"]
            else:
                return ["Traffic Details", "Logs"]
        return ["CPU"]


class VmMonitoringCatalog:
    @staticmethod
    def list_categories() -> List[Dict[str, Any]]:
        output = []
        for key, value in VM_MONITORING_CATALOG.items():
            output.append(
                {
                    "key": key,
                    "label": value["label"],
                    "metric_count": len(value["metrics"]),
                }
            )
        return output

    @staticmethod
    def list_metrics_by_category(category_key: str) -> List[Dict[str, Any]]:
        category = VM_MONITORING_CATALOG.get(category_key)
        if not category:
            return []

        out = []
        for metric_key, metric in category["metrics"].items():
            out.append(
                {
                    "key": metric_key,
                    "label": metric["label"],
                    "description": metric["description"],
                    "unit": metric["unit"],
                    "default_warning": metric["default_warning"],
                    "default_critical": metric["default_critical"],
                    "duration_options": metric["duration_options"],
                    "threshold_type": metric["threshold_type"],
                    "note": metric.get("note"),
                }
            )
        return out

    @staticmethod
    def get_metric_config(metric_key: str) -> Optional[Dict[str, Any]]:
        for category in VM_MONITORING_CATALOG.values():
            if metric_key in category["metrics"]:
                return category["metrics"][metric_key]
        return None


class VmAlertPolicyOrchestrator:
    """
    Creates VM alert policies in Cloud Monitoring using the clean customer-facing catalog.
    """

    @staticmethod
    def _comparison_enum(operator: str):
        if monitoring_v3 is None:
            raise RuntimeError(
                "Missing google-cloud-monitoring. Install: pip install google-cloud-monitoring"
            )

        op = operator.strip().lower()

        # 🚨 THE FIX: Using the correct Google Cloud SDK path for the Enum 🚨
        enum_cls = monitoring_v3.ComparisonType

        mapping = {
            "gt": enum_cls.COMPARISON_GT,
            ">": enum_cls.COMPARISON_GT,
            "lt": enum_cls.COMPARISON_LT,
            "<": enum_cls.COMPARISON_LT,
            "gte": enum_cls.COMPARISON_GT,
            ">=": enum_cls.COMPARISON_GT,
            "lte": enum_cls.COMPARISON_LT,
            "<=": enum_cls.COMPARISON_LT,
        }

        if op not in mapping:
            raise ValueError("Operator must be one of: gt, lt, gte, lte, >, <, >=, <=")

        return mapping[op]

    @staticmethod
    def _aligner_enum(aligner_name: str):
        aligner_cls = monitoring_v3.Aggregation.Aligner
        if not hasattr(aligner_cls, aligner_name):
            raise ValueError(f"Unsupported aligner: {aligner_name}")
        return getattr(aligner_cls, aligner_name)

    @staticmethod
    def _reducer_enum(reducer_name: str):
        # Maps string names to GCP API Enums
        reducers = {
            "REDUCE_SUM": monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
            "REDUCE_MEAN": monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            "REDUCE_MAX": monitoring_v3.Aggregation.Reducer.REDUCE_MAX,
            "REDUCE_MIN": monitoring_v3.Aggregation.Reducer.REDUCE_MIN,
        }
        return reducers.get(reducer_name, monitoring_v3.Aggregation.Reducer.REDUCE_NONE)

    @staticmethod
    def _transform_threshold(transform_name: str, value: float) -> float:
        if transform_name == "percent_to_fraction":
            return value / 100.0
        if transform_name == "gb_to_bytes":
            return value * 1073741824.0
        if transform_name == "mb_to_bytes":
            return value * 1048576.0  # Converts MB to true Bytes for GCP API
        return value

    @staticmethod
    def _get_metric_config(metric_key: str) -> Dict[str, Any]:
        metric_cfg = VmMonitoringCatalog.get_metric_config(metric_key)
        if not metric_cfg:
            raise ValueError(f"Unsupported metric_key: {metric_key}")
        if not metric_cfg.get("gcp_metric"):
            raise ValueError(
                f"Metric '{metric_key}' is customer-visible but not yet wired to a GCP metric."
            )
        return metric_cfg

    @staticmethod
    def create_vm_alert_policy(
        credentials: Credentials,
        project_id: str,
        instance_ids: List[str],
        instance_names: List[str],
        metric_key: str,
        threshold_value: float,
        operator: str,
        duration_seconds: int,
        policy_display_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        if monitoring_v3 is None or duration_pb2 is None:
            raise RuntimeError(
                "Missing dependencies. Install: pip install google-cloud-monitoring protobuf"
            )

        if not instance_ids:
            raise ValueError("At least one instance_id is required.")

        metric_cfg = VmAlertPolicyOrchestrator._get_metric_config(metric_key)

        client = monitoring_v3.AlertPolicyServiceClient(credentials=credentials)
        project_name = f"projects/{project_id}"

        instance_filter = " OR ".join(
            [f'resource.labels.instance_id="{iid}"' for iid in instance_ids]
        )

        filter_str = (
            f"{metric_cfg['resource_type']} AND "
            f"({instance_filter}) AND "
            f"{metric_cfg['gcp_metric']}"
        )

        transformed_threshold = VmAlertPolicyOrchestrator._transform_threshold(
            metric_cfg["transform"], threshold_value
        )

        # --- 1. PREPARE AGGREGATIONS DYNAMICALLY ---
        agg_args = {
            "alignment_period": duration_pb2.Duration(
                seconds=metric_cfg.get("alignment_period", 60)
            ),
            "per_series_aligner": VmAlertPolicyOrchestrator._aligner_enum(
                metric_cfg.get("aligner", "ALIGN_MEAN")
            ),
        }

        # Add reducer if present in the custom config
        if "cross_series_reducer" in metric_cfg:
            agg_args["cross_series_reducer"] = VmAlertPolicyOrchestrator._reducer_enum(
                metric_cfg["cross_series_reducer"]
            )

        aggregations = [monitoring_v3.Aggregation(**agg_args)]

        # --- 2. BUILD THE CONDITION ---
        condition = monitoring_v3.AlertPolicy.Condition(
            display_name=f"{metric_cfg['label']} condition",
            condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
                filter=filter_str,
                comparison=VmAlertPolicyOrchestrator._comparison_enum(operator),
                threshold_value=transformed_threshold,
                duration=duration_pb2.Duration(seconds=duration_seconds),
                aggregations=aggregations,  # Use the prepared list
                trigger=monitoring_v3.AlertPolicy.Condition.Trigger(count=1),
            ),
        )

        # --- 3. BUILD AND PUSH POLICY ---
        if not policy_display_name:
            policy_display_name = f"Lens | {metric_cfg['label']} | {operator} {threshold_value} | {', '.join(instance_names)}"

        policy = monitoring_v3.AlertPolicy(
            display_name=policy_display_name,
            combiner=monitoring_v3.AlertPolicy.ConditionCombinerType.AND,
            conditions=[condition],
            enabled=True,
            documentation=monitoring_v3.AlertPolicy.Documentation(
                content=f"Created by Lens CLI\nMetric: {metric_cfg['label']}",
                mime_type="text/markdown",
            ),
        )

        created = client.create_alert_policy(name=project_name, alert_policy=policy)

        return {
            "message": "Alert policy created successfully",
            "policy_name": created.name,
            "metric_label": metric_cfg["label"],
        }


class VmMetricsOrchestrator:
    @staticmethod
    def get_cpu_utilization(
        credentials: Credentials,
        project_id: str,
        instance_id: str,
        minutes_back: int = 60,
        alignment_seconds: int = 60,
    ) -> Dict[str, Any]:
        if monitoring_v3 is None:
            raise RuntimeError(
                "Missing google-cloud-monitoring. Install: pip install google-cloud-monitoring"
            )

        client = monitoring_v3.MetricServiceClient(credentials=credentials)
        project_name = f"projects/{project_id}"

        end_seconds = int(time.time())
        start_seconds = end_seconds - int(minutes_back * 60)

        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": end_seconds, "nanos": 0},
                "start_time": {"seconds": start_seconds, "nanos": 0},
            }
        )

        filter_str = (
            'metric.type="compute.googleapis.com/instance/cpu/utilization" '
            'AND resource.type="gce_instance" '
            f'AND resource.labels.instance_id="{instance_id}"'
        )

        aggregation = monitoring_v3.Aggregation(
            {
                "alignment_period": {"seconds": int(alignment_seconds)},
                "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
                "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
            }
        )

        results = client.list_time_series(
            request={
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                "aggregation": aggregation,
                "order_by": "timestamp_desc",
            }
        )

        points: List[Dict[str, Any]] = []

        for ts in results:
            for p in ts.points:
                cpu_pct = p.value.double_value * 100.0
                end_sec = to_unix_seconds(p.interval.end_time)

                # FIXED: Future-proof timezone-aware formatting
                ts_iso = datetime.fromtimestamp(end_sec, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )

                points.append({"timestamp": ts_iso, "cpu_percent": round(cpu_pct, 2)})

        points.sort(key=lambda x: x["timestamp"])

        payload = {
            "metric": "CPU Utilization",
            "unit": "Percent",
            "instance_id": instance_id,
            # FIXED: Future-proof timezone-aware formatting
            "from": datetime.fromtimestamp(start_seconds, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "to": datetime.fromtimestamp(end_seconds, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "data": points,
        }

        if not points:
            payload["note"] = (
                "No data points returned. Ensure you used the numeric instance_id (not name), "
                "the VM is running, and the service account has roles/monitoring.viewer."
            )

        return payload


class VmLogsOrchestrator:
    """
    Fetches recent log entries from Google Cloud Logging for a specific resource.
    """

    @staticmethod
    def get_recent_logs(
        credentials: Credentials, project_id: str, instance_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        if logging_v2 is None:
            raise RuntimeError(
                "Missing google-cloud-logging. Install: pip install google-cloud-logging"
            )

        client = logging_v2.Client(project=project_id, credentials=credentials)
        filter_str = f'resource.type="gce_instance" AND resource.labels.instance_id="{instance_id}"'

        entries = client.list_entries(
            filter_=filter_str,
            max_results=limit,
            order_by="timestamp desc",
        )

        logs = []
        for entry in entries:
            # 1. Determine payload safely
            payload = entry.payload
            if payload is None:
                payload = getattr(entry, "text_payload", "")

            if isinstance(payload, dict):
                payload = json.dumps(payload)

            # 2. Build the raw dict
            raw_log = {
                "timestamp": entry.timestamp.isoformat() if entry.timestamp else "N/A",
                "severity": str(entry.severity) if entry.severity else "DEFAULT",
                "log_name": str(entry.log_name) if entry.log_name else "syslog",
                "message": payload,
            }

            # 3. Use your formatting logic immediately
            logs.append(VmLogsOrchestrator.format_log_entry(raw_log))

        return logs  # Correct indentation: return after the loop finishes

    @staticmethod
    def format_log_entry(raw_log: Dict[str, Any]) -> Dict[str, Any]:
        """Cleans up the raw GCP log entry into a structured format."""
        raw_msg = raw_log.get("message", "{}")

        try:
            parsed_msg = json.loads(raw_msg)
            final_msg = parsed_msg.get("message", raw_msg)
        except (json.JSONDecodeError, TypeError):
            final_msg = raw_msg

        # Simplify common log noise
        final_msg = re.sub(
            r"rsyslogd: action '.*' suspended", "rsyslogd action suspended", final_msg
        )
        final_msg = re.sub(
            r"rsyslogd: action '.*' resumed", "rsyslogd action resumed", final_msg
        )

        return {
            "timestamp": raw_log.get("timestamp", "N/A"),
            "severity": raw_log.get("severity", "DEFAULT"),
            "log_name": raw_log.get("log_name", "syslog").split("/")[-1],
            "message": final_msg.strip(),
        }


class VmSystemOrchestrator:
    @staticmethod
    def get_audit_events(
        credentials: Credentials, project_id: str, instance_id: str
    ) -> List[Dict]:
        client = logging_v2.Client(project=project_id, credentials=credentials)
        # Filters for system events like START, STOP, MIGRATE
        filter_str = (
            f'resource.type="gce_instance" AND resource.labels.instance_id="{instance_id}" AND '
            f'logName="projects/{project_id}/logs/cloudaudit.googleapis.com%2Factivity"'
        )
        entries = client.list_entries(
            filter_=filter_str, max_results=5, order_by="timestamp desc"
        )
        events = []
        for entry in entries:
            payload = entry.payload
            events.append(
                {
                    "timestamp": (
                        entry.timestamp.isoformat() if entry.timestamp else None
                    ),
                    "action": (
                        payload.get("methodName", "unknown")
                        if isinstance(payload, dict)
                        else "unknown"
                    ),
                    "user": (
                        payload.get("authenticationInfo", {}).get(
                            "principalEmail", "system"
                        )
                        if isinstance(payload, dict)
                        else "system"
                    ),
                }
            )
        return events

    @staticmethod
    def list_vm_alerts(
        credentials: Credentials, project_id: str, instance_id: str
    ) -> List[Dict]:
        client = monitoring_v3.AlertPolicyServiceClient(credentials=credentials)
        project_name = f"projects/{project_id}"
        policies = client.list_alert_policies(name=project_name)

        vm_policies = []
        for policy in policies:
            # Check if this policy's filter mentions this specific VM
            for condition in policy.conditions:
                if (
                    condition.condition_threshold
                    and instance_id in condition.condition_threshold.filter
                ):
                    vm_policies.append(
                        {
                            "name": policy.display_name,
                            "enabled": policy.enabled,
                            "id": policy.name.split("/")[-1],
                        }
                    )
                    break  # Only add it once per policy
        return vm_policies


class InventoryOrchestrator:
    """
    This is the "Console data fetcher" for your current 4-service scope.

    Later, when you add monitoring metrics, you'll create a parallel:
      ObservabilityOrchestrator(resource -> categories -> metric queries)
    """

    @staticmethod
    def fetch_project_inventory(
        credentials: Credentials, project_id: str
    ) -> InventoryResult:
        services: Dict[str, List[NormalizedResource]] = {
            "vm": [],
            "gke": [],
            "database": [],
            "networking": [],
        }
        errors: List[str] = []

        # VM
        try:
            services["vm"] = ComputeInventory.list_instances(credentials, project_id)
        except Exception as e:
            errors.append(f"VM inventory failed: {e}")

        # GKE
        try:
            services["gke"] = GkeInventory.list_clusters(credentials, project_id)
        except Exception as e:
            errors.append(f"GKE inventory failed: {e}")

        # Database (Cloud SQL)
        try:
            services["database"] = CloudSqlInventory.list_instances(
                credentials, project_id
            )
        except Exception as e:
            errors.append(f"Cloud SQL inventory failed: {e}")

        # Networking
        try:
            net: List[NormalizedResource] = []
            net.extend(NetworkingInventory.list_networks(credentials, project_id))
            net.extend(NetworkingInventory.list_subnets(credentials, project_id))
            net.extend(NetworkingInventory.list_firewall_rules(credentials, project_id))
            net.extend(
                NetworkingInventory.list_routers_and_nats(credentials, project_id)
            )
            services["networking"] = net
        except Exception as e:
            errors.append(f"Networking inventory failed: {e}")

        return InventoryResult(project_id=project_id, services=services, errors=errors)


def inventory_to_dict(inv: InventoryResult) -> Dict[str, Any]:
    """
    Convert InventoryResult to JSON serializable dict.
    """
    out = {
        "project_id": inv.project_id,
        "errors": inv.errors,
        "services": {},
    }
    for svc, items in inv.services.items():
        out["services"][svc] = [asdict(r) for r in items]
    return out


def choose_project_interactive(creds: Credentials) -> str:
    projects = ProjectDiscovery.list_accessible_projects(creds)

    if not projects:
        # Changed to a print/warning instead of a hard crash, so they can still manually type one!
        print("\n⚠️  Warning: No accessible projects found automatically.")
        print("    (You may lack roles/resourcemanager.projectViewer).")
    else:
        print("\n" + "=" * 70)
        print("📁 PROJECT SELECTOR")
        print("=" * 70)
        for i, pid in enumerate(projects):
            print(f"[{i}] {pid}")

    while True:
        # Added a clean exit instruction
        raw = input(
            "\nEnter the project index (or type a project_id) [Press Enter to exit]: "
        ).strip()

        # 1. Clean Exit Strategy
        if not raw:
            print("Operation cancelled. Exiting.")
            sys.exit(0)

        # 2. Index Selection
        if raw.isdigit():
            idx = int(raw)
            if 0 <= idx < len(projects):
                return projects[idx]
            print("Invalid index. Try again.")

        # 3. Manual Project ID Entry
        else:
            if raw not in projects:
                # If they type a custom project ID not in the list, warn them but let them proceed!
                print(
                    f"⚠️  Note: '{raw}' was not in the discovered list, but we will attempt to use it."
                )
            return raw


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="GCP Console Inventory & Metrics Fetcher")
    p.add_argument(
        "--sa-file", help="Path to service account JSON key file", default=None
    )
    p.add_argument("--sa-json", help="Raw service account JSON (string)", default=None)
    p.add_argument("--sa-b64", help="Base64-encoded service account JSON", default=None)
    p.add_argument(
        "--project",
        help="Project ID (if omitted, tries to list projects)",
        default=None,
    )
    p.add_argument(
        "--list-projects", action="store_true", help="List accessible projects and exit"
    )

    # --- NEW: Metrics & Logs Testing Arguments ---
    p.add_argument(
        "--test-vm-cpu",
        help="Provide an instance ID to test fetching CPU metrics.",
        default=None,
    )
    p.add_argument(
        "--test-vm-logs",
        help="Provide an instance ID to test fetching recent logs.",
        default=None,
    )

    return p.parse_args()


def status_dot(status: str) -> str:
    s = (status or "").upper()
    if s == "RUNNING":
        return "\033[92m●\033[0m RUNNING"
    if s == "STOPPED":
        return "\033[91m●\033[0m STOPPED"
    if s == "TERMINATED":
        return "\033[93m●\033[0m TERMINATED"
    return f"● {s}"


def to_unix_seconds(ts_obj) -> int:
    """
    Supports both:
    - protobuf Timestamp (has .seconds)
    - DatetimeWithNanoseconds / datetime (has .timestamp())
    """
    if ts_obj is None:
        return 0

    sec = getattr(ts_obj, "seconds", None)
    if sec is not None:
        return int(sec)

    # datetime-like object
    if hasattr(ts_obj, "timestamp"):
        return int(ts_obj.timestamp())

    # last resort
    return int(time.time())


def choose_metric_from_catalog_interactive(selected_tab: str) -> Optional[str]:

    # Pass the tab directly (e.g. "CPU" or "Disk") to get the right metrics
    metrics = VmMonitoringCatalog.list_metrics_by_category(selected_tab)

    if not metrics:
        print(f"\n🚧 Metrics for the '{selected_tab}' tab are not in the catalog yet.")
        return None

    print(f"\n--- {selected_tab} Metrics ---")
    metric_map = {}

    for i, metric in enumerate(metrics, start=1):
        metric_map[str(i)] = metric["key"]
        warn = metric["default_warning"]
        crit = metric["default_critical"]

        print(f"{i}: {metric['label']} ({metric['description']})")
        if warn is not None or crit is not None:
            print(
                f"   Warning: {warn} {metric['unit']} | Critical: {crit} {metric['unit']}"
            )

    metric_choice = input("\nEnter metric number (or press Enter to go back): ").strip()

    # If the user just presses Enter, metric_choice will be empty ("")
    if not metric_choice:
        return None

    return metric_map.get(metric_choice)


def status_dot(status: str) -> str:
    if not status:
        return "⚪ UNKNOWN"

    status = status.upper()
    if status == "RUNNING":
        return "🟢 RUNNING"
    elif status in ("TERMINATED", "STOPPED"):
        return "🔴 TERMINATED"
    elif status in ("PROVISIONING", "STAGING", "STARTING"):
        return "🟡 STARTING"
    elif status == "STOPPING":
        return "🟠 STOPPING"
    elif status == "SUSPENDED":
        return "⏸️ SUSPENDED"
    else:
        return f"⚪ {status}"


def choose_threshold_interactive(metric_key: str) -> Optional[float]:
    metric_cfg = VmMonitoringCatalog.get_metric_config(metric_key)
    if not metric_cfg:
        return None

    try:
        raw = input(
            f"Enter threshold value in {metric_cfg['unit']} "
            f"(warning default {metric_cfg['default_warning']}, critical default {metric_cfg['default_critical']}): "
        ).strip()

        # --- ADD THESE TWO LINES ---
        if not raw:
            return float(metric_cfg["default_warning"])
        # ---------------------------

        return float(raw)
    except ValueError:
        return None


def _read_float(prompt: str, default: Optional[float] = None) -> float:
    while True:
        raw = input(prompt).strip()
        if not raw and default is not None:
            return default
        try:
            return float(raw)
        except ValueError:
            print("Invalid input. Please enter a numeric value.")


def _read_int(prompt: str, default: Optional[int] = None, min_value: int = 60) -> int:
    while True:
        raw = input(prompt).strip()
        if not raw and default is not None:
            return default
        try:
            value = int(raw)
            # 1. Check Minimum Value
            if value < min_value:
                print(f"Value must be at least {min_value} seconds for GCP Monitoring.")
                continue

            # 2. 🚨 THE FIX: Check if it's a multiple of 60! 🚨
            if value % 60 != 0:
                print(
                    "Error: GCP API requires this time to be an exact multiple of 60 (e.g., 60, 120, 300)."
                )
                continue

            return value
        except ValueError:
            print("Invalid input. Please enter a whole number.")


def configure_custom_cpu_metric() -> Dict[str, Any]:
    print("\n--- Custom CPU Metric Configuration ---")
    alert_name = input("\nEnter alert name:\n> ").strip() or "custom-cpu-alert"

    print("\nEnter metric type:")
    print("1: CPU Utilization")
    print("2: CPU Idle")
    print("3: CPU Load")
    print("4: CPU Steal Time")
    m_type_choice = input("> ").strip()

    if m_type_choice == "3":
        print("\nSelect load window:")
        print("1: 1 minute")
        print("2: 5 minutes")
        print("3: 15 minutes")
        load_choice = input("> ").strip()

        if load_choice == "2":
            gcp_metric = 'metric.type="agent.googleapis.com/cpu/load_5m"'
            metric_label = "CPU Load (5m)"
        elif load_choice == "3":
            gcp_metric = 'metric.type="agent.googleapis.com/cpu/load_15m"'
            metric_label = "CPU Load (15m)"
        else:
            gcp_metric = 'metric.type="agent.googleapis.com/cpu/load_1m"'
            metric_label = "CPU Load (1m)"

        unit = "load"
        transform = "identity"

    else:
        metric_map = {
            "1": (
                'metric.type="compute.googleapis.com/instance/cpu/utilization"',
                "%",
                "percent_to_fraction",
                "CPU Utilization",
            ),
            "2": (
                'metric.type="agent.googleapis.com/cpu/idle"',
                "%",
                "identity",
                "CPU Idle",
            ),
            "4": (
                'metric.type="agent.googleapis.com/cpu/steal_time"',
                "%",
                "identity",
                "CPU Steal Time",
            ),
        }

        gcp_metric, unit, transform, metric_label = metric_map.get(
            m_type_choice, metric_map["1"]
        )

    print("\nSelect operator:")
    print("1: >")
    print("2: <")
    print("3: >=")
    print("4: <=")
    op_choice = input("> ").strip()
    op_map = {"1": ">", "2": "<", "3": ">=", "4": "<="}
    operator = op_map.get(op_choice, ">")

    threshold_val = _read_float("\nEnter threshold value:\n> ")
    eval_window = _read_int("\nEnter evaluation window (seconds):\n> ", min_value=60)
    align_period = _read_int("\nEnter alignment period (seconds):\n> ", min_value=60)

    return {
        "alert_name": alert_name,
        "label": metric_label,
        "unit": unit,
        "gcp_metric": gcp_metric,
        "operator": operator,
        "threshold_value": threshold_val,
        "duration_seconds": eval_window,
        "alignment_period": align_period,
        "transform": transform,
    }


def configure_custom_memory_metric() -> Dict[str, Any]:
    print("\n--- Custom Memory Metric Configuration ---")
    alert_name = input("\nEnter alert name:\n> ").strip() or "custom-memory-alert"

    print("\nEnter metric type:")
    print("1: Memory Utilization (%)")
    print("2: Available Memory (GB)")
    print("3: Swap Utilization (%)")
    print("4: Memory Used (GB)")
    print("5: Cached Memory (GB)")
    m_type_choice = input("> ").strip()

    metric_map = {
        "1": {
            "label": "Memory Utilization",
            "gcp_metric": 'metric.type="agent.googleapis.com/memory/percent_used" AND metric.labels.state="used"',
            "unit": "%",
            "transform": "identity",
            "default_operator": ">=",
        },
        "2": {
            "label": "Available Memory",
            "gcp_metric": 'metric.type="agent.googleapis.com/memory/bytes_used" AND metric.labels.state="free"',
            "unit": "GB",
            "transform": "gb_to_bytes",
            "default_operator": "<=",
        },
        "3": {
            "label": "Swap Utilization",
            "gcp_metric": 'metric.type="agent.googleapis.com/swap/percent_used"',
            "unit": "%",
            "transform": "identity",
            "default_operator": ">=",
        },
        "4": {
            "label": "Memory Used",
            "gcp_metric": 'metric.type="agent.googleapis.com/memory/bytes_used" AND metric.labels.state="used"',
            "unit": "GB",
            "transform": "gb_to_bytes",
            "default_operator": ">=",
        },
        "5": {
            "label": "Cached Memory",
            "gcp_metric": 'metric.type="agent.googleapis.com/memory/bytes_used" AND metric.labels.state="cached"',
            "unit": "GB",
            "transform": "gb_to_bytes",
            "default_operator": ">=",
        },
    }

    selected = metric_map.get(m_type_choice, metric_map["1"])

    print("\nSelect operator:")
    print("1: >")
    print("2: <")
    print("3: >=")
    print("4: <=")
    op_choice = input("> ").strip()

    op_map = {"1": ">", "2": "<", "3": ">=", "4": "<="}
    operator = op_map.get(op_choice, selected["default_operator"])

    threshold_val = _read_float(f"\nEnter threshold value ({selected['unit']}):\n> ")
    eval_window = _read_int("\nEnter evaluation window (seconds):\n> ", min_value=60)
    align_period = _read_int("\nEnter alignment period (seconds):\n> ", min_value=60)

    if align_period > eval_window:
        print(
            "Alignment period cannot be greater than evaluation window. Setting it equal to evaluation window."
        )
        align_period = eval_window

    return {
        "alert_name": alert_name,
        "label": selected["label"],
        "unit": selected["unit"],
        "gcp_metric": selected["gcp_metric"],
        "operator": operator,
        "threshold_value": threshold_val,
        "duration_seconds": eval_window,
        "alignment_period": align_period,
        "transform": selected["transform"],
    }


def configure_custom_disk_metric() -> Dict[str, Any]:
    print("\n--- Custom Disk Metric Configuration ---")
    alert_name = input("\nEnter alert name:\n> ").strip() or "custom-disk-alert"

    print("\nEnter metric type:")
    print("1: Disk Usage (%)")
    print("2: Disk Read IOPS (ops/s)")
    print("3: Disk Write IOPS (ops/s)")
    print("4: Disk Read Throughput (MB/s)")
    print("5: Disk Write Throughput (MB/s)")
    print("6: Disk Read Latency (ms)")
    print("7: Disk Write Latency (ms)")
    m_type_choice = input("> ").strip()

    metric_map = {
        "1": {
            "label": "Disk Usage",
            "gcp_metric": 'metric.type="agent.googleapis.com/disk/percent_used" AND metric.labels.state="used"',
            "unit": "%",
            "transform": "identity",
            "aligner": "ALIGN_MEAN",
            "default_operator": ">=",
        },
        "2": {
            "label": "Disk Read IOPS",
            "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/read_ops_count"',
            "unit": "ops/s",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
        "3": {
            "label": "Disk Write IOPS",
            "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/write_ops_count"',
            "unit": "ops/s",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
        "4": {
            "label": "Disk Read Throughput",
            "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/read_bytes_count"',
            "unit": "MB/s",
            "transform": "mb_to_bytes",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
        "5": {
            "label": "Disk Write Throughput",
            "gcp_metric": 'metric.type="compute.googleapis.com/instance/disk/write_bytes_count"',
            "unit": "MB/s",
            "transform": "mb_to_bytes",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
        "6": {
            "label": "Disk Read Latency",
            "gcp_metric": 'metric.type="agent.googleapis.com/disk/operation_time" AND metric.labels.direction="read"',
            "unit": "ms",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
        "7": {
            "label": "Disk Write Latency",
            "gcp_metric": 'metric.type="agent.googleapis.com/disk/operation_time" AND metric.labels.direction="write"',
            "unit": "ms",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
    }

    selected = metric_map.get(m_type_choice, metric_map["1"])

    print("\nSelect operator:")
    print("1: >")
    print("2: <")
    print("3: >=")
    print("4: <=")
    op_choice = input("> ").strip()

    op_map = {"1": ">", "2": "<", "3": ">=", "4": "<="}
    operator = op_map.get(op_choice, selected["default_operator"])

    threshold_val = _read_float(f"\nEnter threshold value ({selected['unit']}):\n> ")
    eval_window = _read_int("\nEnter evaluation window (seconds):\n> ", min_value=60)
    align_period = _read_int("\nEnter alignment period (seconds):\n> ", min_value=60)

    if align_period > eval_window:
        print(
            "Alignment period cannot be greater than evaluation window. Setting it equal to evaluation window."
        )
        align_period = eval_window

    return {
        "alert_name": alert_name,
        "label": selected["label"],
        "unit": selected["unit"],
        "gcp_metric": selected["gcp_metric"],
        "operator": operator,
        "threshold_value": threshold_val,
        "duration_seconds": eval_window,
        "alignment_period": align_period,
        "transform": selected["transform"],
        "aligner": selected["aligner"],
    }


def configure_custom_network_metric(creds, project_id, network_name) -> Dict[str, Any]:
    print("\n--- Custom Network Metric Configuration ---")
    alert_name = input("\nEnter alert name:\n> ").strip() or "custom-network-alert"

    print("\nEnter metric type:")
    print("1: Network In (MB/s)")
    print("2: Network Out (MB/s)")
    print("3: Active TCP Connections")
    print("4: Network Errors (errors/s)")
    m_type_choice = input("> ").strip()

    metric_map = {
        "1": {
            "label": "Network Incoming",
            "gcp_metric": 'metric.type="compute.googleapis.com/instance/network/received_bytes_count"',
            "unit": "MB/s",
            "transform": "mb_to_bytes",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
        "2": {
            "label": "Network Outgoing",
            "gcp_metric": 'metric.type="compute.googleapis.com/instance/network/sent_bytes_count"',
            "unit": "MB/s",
            "transform": "mb_to_bytes",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
        "3": {
            "label": "TCP Connections",
            "gcp_metric": 'metric.type="agent.googleapis.com/network/tcp_connections"',
            "unit": "connections",
            "transform": "identity",
            "aligner": "ALIGN_MEAN",
            "default_operator": ">=",
        },
        "4": {
            "label": "Network Errors",
            # 👇 CHANGE THIS LINE 👇
            "gcp_metric": 'metric.type="agent.googleapis.com/interface/errors"',
            "unit": "errors/s",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
    }

    selected = metric_map.get(m_type_choice, metric_map["1"])

    # ========================================================
    # 🟢 NEW FEATURE: FETCH CURRENT BASELINE FOR CONTEXT
    # ========================================================
    from google.cloud import monitoring_v3
    import time

    print(
        f"\n📊 Fetching recent average for {selected['label']} to help you set a threshold..."
    )
    try:
        client = monitoring_v3.MetricServiceClient(credentials=creds)
        clean_metric = (
            selected["gcp_metric"].replace("metric.type=", "").replace('"', "").strip()
        )

        # Broad filter to grab average across the project/network to give them a baseline
        filter_str = f'metric.type="{clean_metric}"'
        now = int(time.time())

        aligner_enum = getattr(monitoring_v3.Aggregation.Aligner, selected["aligner"])

        results = client.list_time_series(
            request={
                "name": f"projects/{project_id}",
                "filter": filter_str,
                "interval": monitoring_v3.TimeInterval(
                    {
                        "end_time": {"seconds": now},
                        "start_time": {"seconds": now - 3600},  # Look back 1 hour
                    }
                ),
                "aggregation": monitoring_v3.Aggregation(
                    {
                        "alignment_period": {"seconds": 3600},  # 1 big 1-hour bucket
                        "per_series_aligner": aligner_enum,
                        "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_MEAN,
                    }
                ),
            }
        )

        val = None
        for ts in results:
            for p in ts.points:
                if p.value.HasField("double_value"):
                    val = p.value.double_value
                elif p.value.HasField("int64_value"):
                    val = p.value.int64_value
                break
            if val is not None:
                break

        if val is not None:
            # If GCP returned Bytes but the user expects MB, reverse the math for display!
            if selected.get("transform") == "mb_to_bytes":
                val = val / 1048576.0
            print(
                f"✅ Baseline Context: Over the last hour, the average is roughly {val:.2f} {selected['unit']}."
            )
        else:
            print(
                "⚠️ No active traffic/data found in the last hour to establish a baseline."
            )

    except Exception as e:
        print(
            f"⚠️ Could not fetch live data for context. Proceeding to threshold configuration..."
        )
    # ========================================================

    print("\nSelect operator:\n1: >\n2: <\n3: >=\n4: <=")
    op_choice = input("> ").strip()
    op_map = {"1": ">", "2": "<", "3": ">=", "4": "<="}
    operator = op_map.get(op_choice, selected["default_operator"])

    threshold_val = _read_float(f"\nEnter threshold value ({selected['unit']}):\n> ")
    eval_window = _read_int("\nEnter evaluation window (seconds):\n> ", min_value=60)
    align_period = _read_int("\nEnter alignment period (seconds):\n> ", min_value=60)

    if align_period > eval_window:
        print("Alignment period > window. Resetting.")
        align_period = eval_window

    return {
        "alert_name": alert_name,
        "label": selected["label"],
        "unit": selected["unit"],
        "gcp_metric": selected["gcp_metric"],
        "operator": operator,
        "threshold_value": threshold_val,
        "duration_seconds": eval_window,
        "alignment_period": align_period,
        "transform": selected["transform"],
        "aligner": selected["aligner"],  # 🟢 Ensure this is passed!
    }


def configure_custom_subnet_metric(creds, project_id, subnet_name) -> Dict[str, Any]:
    print(f"\n--- Subnet Alert Configuration ({subnet_name}) ---")
    alert_name = input("\nEnter alert name:\n> ").strip() or f"{subnet_name}-alert"

    print("\nEnter metric type:")
    print("1: High Traffic Volume (Flow Logs)")
    print("2: High Log Entry Count")
    m_type_choice = input("> ").strip()

    metric_map = {
        "1": {
            "label": "High Traffic Volume",
            "gcp_metric": f'metric.type="logging.googleapis.com/byte_count" AND resource.type="gce_subnetwork" AND resource.labels.subnetwork_name="{subnet_name}"',
            "unit": "Bytes/s",
            "transform": "mb_to_bytes",  # User inputs MB, GCP needs Bytes
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
            "resource_type": "gce_subnetwork",
        },
        "2": {
            "label": "High Log Entry Count",
            "gcp_metric": f'metric.type="logging.googleapis.com/log_entry_count" AND resource.type="gce_subnetwork" AND resource.labels.subnetwork_name="{subnet_name}"',
            "unit": "logs/s",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
            "resource_type": "gce_subnetwork",
        },
    }

    selected = metric_map.get(m_type_choice, metric_map["1"])

    print("\nSelect operator:\n1: >\n2: <\n3: >=\n4: <=")
    op_choice = input("> ").strip()
    op_map = {"1": ">", "2": "<", "3": ">=", "4": "<="}
    operator = op_map.get(op_choice, selected["default_operator"])

    threshold_val = _read_float(
        f"\nEnter threshold value ({selected['unit'].replace('Bytes', 'MB')}):\n> "
    )
    eval_window = _read_int("\nEnter evaluation window (seconds):\n> ", min_value=60)
    align_period = _read_int("\nEnter alignment period (seconds):\n> ", min_value=60)

    if align_period > eval_window:
        align_period = eval_window

    return {
        "alert_name": alert_name,
        "label": selected["label"],
        "unit": selected["unit"],
        "gcp_metric": selected["gcp_metric"],
        "operator": operator,
        "threshold_value": threshold_val,
        "duration_seconds": eval_window,
        "alignment_period": align_period,
        "transform": selected["transform"],
        "aligner": selected["aligner"],
        "resource_type": selected["resource_type"],  # 🟢 Pass explicit resource type
    }


def configure_custom_process_metric() -> Dict[str, Any]:
    print("\n--- Custom Process Metric Configuration ---")
    alert_name = input("\nEnter alert name:\n> ").strip() or "custom-proc-alert"

    print("\nEnter metric type:")
    print("1: Total Processes")
    print("2: Zombie Processes")
    print("3: Memory Usage (RSS MB)")
    print("4: CPU Usage (cpu-seconds/s)")
    m_type_choice = input("> ").strip()

    metric_map = {
        "1": {
            "label": "Total Process Count",
            "gcp_metric": 'metric.type="agent.googleapis.com/processes/count_by_state"',
            "unit": "count",
            "transform": "identity",
            "aligner": "ALIGN_MEAN",
            "default_operator": ">=",
            "cross_series_reducer": "REDUCE_SUM",
        },
        "2": {
            "label": "Zombie Process Count",
            "gcp_metric": 'metric.type="agent.googleapis.com/processes/count_by_state" AND metric.labels.state="zombie"',
            "unit": "count",
            "transform": "identity",
            "aligner": "ALIGN_MEAN",
            "default_operator": ">=",
        },
        "3": {
            "label": "Process Memory (RSS)",
            "gcp_metric": 'metric.type="agent.googleapis.com/processes/rss_usage"',
            "unit": "MB",
            "transform": "mb_to_bytes",
            "aligner": "ALIGN_MEAN",
            "default_operator": ">=",
        },
        "4": {
            "label": "Process CPU Usage",
            "gcp_metric": 'metric.type="agent.googleapis.com/processes/cpu_time"',
            "unit": "cpu-seconds/s",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
        },
    }

    selected = metric_map.get(m_type_choice, metric_map["1"])

    print("\nSelect operator:\n1: >\n2: <\n3: >=\n4: <=")
    op_choice = input("> ").strip()
    op_map = {"1": ">", "2": "<", "3": ">=", "4": "<="}
    operator = op_map.get(op_choice, selected["default_operator"])

    threshold_val = _read_float(f"\nEnter threshold value ({selected['unit']}):\n> ")
    eval_window = _read_int("\nEnter evaluation window (seconds):\n> ", min_value=60)
    align_period = _read_int("\nEnter alignment period (seconds):\n> ", min_value=60)

    if align_period > eval_window:
        align_period = eval_window

    result = {
        "alert_name": alert_name,
        "label": selected["label"],
        "unit": selected["unit"],
        "gcp_metric": selected["gcp_metric"],
        "operator": operator,
        "threshold_value": threshold_val,
        "duration_seconds": eval_window,
        "alignment_period": align_period,
        "transform": selected["transform"],
        "aligner": selected["aligner"],
    }

    if "cross_series_reducer" in selected:
        result["cross_series_reducer"] = selected["cross_series_reducer"]

    return result


def handle_networking_navigation(selected_res, cat_key, creds, project_id):
    """Handles the 4-tab exploration for Networking resources."""
    while True:
        tabs = NetworkingCatalog.get_tabs(cat_key)
        print(f"\nSelect a tab to open for {selected_res.name}:")
        tab_map = {str(i + 1): t for i, t in enumerate(tabs)}
        for k, v in tab_map.items():
            print(f"{k}: {v}")

        tab_choice = input("\nEnter tab number (or press Enter to go back): ").strip()
        if not tab_choice:
            return

        selected_tab = tab_map.get(tab_choice)
        if not selected_tab:
            continue

        # ROUTING: Call specific logic based on tab
        if selected_tab == "Overview":
            # ... (your overview logic here) ...
            input("\nPress Enter to return to tabs...")
        elif selected_tab == "Alerts":
            # ... (your alerts logic here) ...
            handle_network_alerts(selected_res, creds, project_id)
        # Add other tabs here...


class NetworkMetricsOrchestrator:
    @staticmethod
    def _escape_monitoring_filter_value(value: str) -> str:
        """Helper to prevent filter syntax errors if names contain quotes/slashes."""
        return value.replace("\\", "\\\\").replace('"', '\\"')

    @staticmethod
    def get_firewall_metrics(credentials, project_id, firewall_name, lookback_hours=24):
        """
        Tier 1 firewall traffic metrics using Firewall Insights.
        """
        from google.cloud import monitoring_v3
        from datetime import datetime, timedelta, timezone

        client = monitoring_v3.MetricServiceClient(credentials=credentials)
        project_name = f"projects/{project_id}"

        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=max(1, int(lookback_hours)))

        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {
                    "seconds": int(now.timestamp()),
                    "nanos": now.microsecond * 1000,
                },
                "start_time": {
                    "seconds": int(start.timestamp()),
                    "nanos": start.microsecond * 1000,
                },
            }
        )

        def _sum_int64_points(series_iter):
            total = 0
            found = False
            for series in series_iter:
                for point in series.points:
                    total += int(point.value.int64_value)
                    found = True
            return found, total

        def _latest_int64_point(series_iter):
            latest_ts = None
            latest_value = None

            for series in series_iter:
                for point in series.points:
                    end_time = point.interval.end_time
                    point_ts = datetime.fromtimestamp(
                        end_time.timestamp(), tz=timezone.utc
                    )
                    value = int(point.value.int64_value)

                    if latest_ts is None or point_ts > latest_ts:
                        latest_ts = point_ts
                        latest_value = value

            return latest_ts, latest_value

        try:
            # 🟢 Apply the safety escape!
            safe_firewall_name = (
                NetworkMetricsOrchestrator._escape_monitoring_filter_value(
                    firewall_name
                )
            )

            # 1) Hit count over requested lookback window
            hit_filter = (
                'metric.type="firewallinsights.googleapis.com/subnet/firewall_hit_count" '
                f'AND metric.labels.firewall_name="{safe_firewall_name}"'
            )

            hit_iter = client.list_time_series(
                request={
                    "name": project_name,
                    "filter": hit_filter,
                    "interval": interval,
                    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                }
            )

            hits_found, total_hits = _sum_int64_points(hit_iter)

            # 2) Last used timestamp
            last_used_start = now - timedelta(days=7)
            last_used_interval = monitoring_v3.TimeInterval(
                {
                    "end_time": {
                        "seconds": int(now.timestamp()),
                        "nanos": now.microsecond * 1000,
                    },
                    "start_time": {
                        "seconds": int(last_used_start.timestamp()),
                        "nanos": last_used_start.microsecond * 1000,
                    },
                }
            )

            last_used_filter = (
                'metric.type="firewallinsights.googleapis.com/subnet/firewall_last_used_timestamp" '
                f'AND metric.labels.firewall_name="{safe_firewall_name}"'
            )

            last_used_iter = client.list_time_series(
                request={
                    "name": project_name,
                    "filter": last_used_filter,
                    "interval": last_used_interval,
                    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                }
            )

            _, last_used_epoch = _latest_int64_point(last_used_iter)

            last_used_readable = None
            if last_used_epoch:
                last_used_dt = datetime.fromtimestamp(last_used_epoch, tz=timezone.utc)
                last_used_readable = last_used_dt.strftime("%Y-%m-%d %H:%M:%S UTC")

            if not hits_found and not last_used_epoch:
                return {
                    "status": "no_data",
                    "data": {
                        "lookback_hours": lookback_hours,
                        "total_hits": 0,
                        "last_used_epoch": None,
                        "last_used_readable": None,
                    },
                    "message": (
                        "⚠️ No Firewall Insights metric points were returned for this rule "
                        f"in the selected window ({lookback_hours}h). "
                        "This can mean no recent TCP/UDP hits, logging was enabled only recently, "
                        "or data is not yet visible in Monitoring."
                    ),
                }

            return {
                "status": "success",
                "data": {
                    "lookback_hours": lookback_hours,
                    "total_hits": total_hits,
                    "last_used_epoch": last_used_epoch,
                    "last_used_readable": last_used_readable,
                },
                "message": "ok",
            }

        except Exception as e:
            return {
                "status": "error",
                "data": None,
                "message": f"❌ Failed to fetch Firewall Insights metrics: {e}",
            }

    @staticmethod
    def get_subnet_metrics(credentials, project_id, subnet_name):
        from google.cloud import monitoring_v3
        import time

        client = monitoring_v3.MetricServiceClient(credentials=credentials)
        now = int(time.time())

        # 🟢 Apply the safety escape here too!
        safe_subnet_name = NetworkMetricsOrchestrator._escape_monitoring_filter_value(
            subnet_name
        )

        filter_str = (
            f'metric.type="logging.googleapis.com/byte_count" AND '
            f'resource.type="gce_subnetwork" AND '
            f'resource.labels.subnetwork_name="{safe_subnet_name}"'
        )

        try:
            results = client.list_time_series(
                request={
                    "name": f"projects/{project_id}",
                    "filter": filter_str,
                    "interval": monitoring_v3.TimeInterval(
                        {
                            "end_time": {"seconds": now},
                            "start_time": {"seconds": now - 3600},
                        }
                    ),
                    "aggregation": monitoring_v3.Aggregation(
                        {
                            "alignment_period": {"seconds": 3600},
                            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
                            "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
                        }
                    ),
                }
            )

            val = 0
            for ts in results:
                for p in ts.points:
                    val += p.value.double_value
                    val += p.value.int64_value

            # 🟢 Now returning structural data alongside the string message!
            if val > 0:
                mb_s = val / 1048576.0
                return {
                    "status": "success",
                    "data": {
                        "traffic_mb_per_sec": round(mb_s, 2),
                        "window_hours": 1,
                    },
                    "message": f"📊 Average Flow Log Traffic over last hour: {mb_s:.2f} MB/s",
                }
            else:
                return {
                    "status": "empty",
                    "data": {
                        "traffic_mb_per_sec": 0.0,
                        "window_hours": 1,
                    },
                    "message": "⚠️ No flow log metric data found for this subnet.",
                }

        except Exception as e:
            return {"status": "error", "message": f"❌ Failed to fetch metrics: {e}"}


def choose_operator_interactive() -> Optional[str]:
    print("\n[Step 2] Select condition operator:")
    print("1: Greater Than (>)")
    print("2: Less Than (<)")
    print("3: Greater Than or Equal To (>=)")
    print("4: Less Than or Equal To (<=)")

    choice = input("\nEnter operator number: ").strip()
    mapping = {"1": ">", "2": "<", "3": ">=", "4": "<="}
    return mapping.get(choice)


def choose_duration_interactive() -> Optional[int]:
    print(
        "\n[Step 4] Select trigger duration (how long the threshold must be breached):"
    )
    print("1: 1 Minute")
    print("2: 5 Minutes")
    print("3: 10 Minutes")
    print("4: 15 Minutes")

    choice = input("\nEnter duration number: ").strip()
    mapping = {"1": 60, "2": 300, "3": 600, "4": 900}
    return mapping.get(choice)


def show_vpc_subnets(selected_vpc, creds, project_id):
    print("\n--- Loading Subnets ---")
    subnets = NetworkOrchestrator.list_resources("subnet", creds, project_id)
    related = [s for s in subnets if s.raw.get("network") == selected_vpc.name]

    print(f"\nViewing Subnets related to {selected_vpc.name}")
    print("=" * 70)

    if not related:
        print("No subnets attached to this VPC.")
    else:
        for i, subnet in enumerate(related, start=1):
            print(f"{i}. Name: {subnet.name}")
            print(f"   Region: {subnet.location}")
            print(f"   CIDR: {subnet.raw.get('ip_cidr_range', 'N/A')}")
            print(
                f"   Private Google Access: {subnet.raw.get('private_ip_google_access', 'N/A')}"
            )
            print("-" * 70)


def show_vpc_firewalls(selected_vpc, creds, project_id):
    print("\n--- Loading Firewall Rules ---")
    firewalls = NetworkOrchestrator.list_resources("firewall", creds, project_id)
    related = [f for f in firewalls if f.raw.get("network") == selected_vpc.name]

    print(f"\nViewing Firewalls related to {selected_vpc.name}")
    print("=" * 70)

    if not related:
        print("No firewall rules attached to this VPC.")
    else:
        for i, fw in enumerate(related, start=1):
            print(f"{i}. Name: {fw.name}")
            print(f"   Direction: {fw.raw.get('direction', 'N/A')}")
            print(f"   Priority: {fw.raw.get('priority', 'N/A')}")
            print(
                f"   Source Ranges: {', '.join(fw.raw.get('source_ranges', [])) or 'N/A'}"
            )
            print(
                f"   Target Tags: {', '.join(fw.raw.get('target_tags', [])) or 'N/A'}"
            )

            allowed = fw.raw.get("allowed", [])
            denied = fw.raw.get("denied", [])

            if allowed:
                print("   Allowed:")
                for rule in allowed:
                    proto = rule.get("IPProtocol", "N/A")
                    ports = ", ".join(rule.get("ports", [])) or "all"
                    print(f"     - {proto}: {ports}")

            if denied:
                print("   Denied:")
                for rule in denied:
                    proto = rule.get("IPProtocol", "N/A")
                    ports = ", ".join(rule.get("ports", [])) or "all"
                    print(f"     - {proto}: {ports}")

            print("-" * 70)


def show_vpc_routes(selected_vpc, creds, project_id):
    print("\n--- Loading Routes ---")
    routes = NetworkOrchestrator.list_resources("route", creds, project_id)
    related = [r for r in routes if r.raw.get("network") == selected_vpc.name]

    print(f"\nViewing Routes related to {selected_vpc.name}")
    print("=" * 70)

    if not related:
        print("No routes attached to this VPC.")
    else:
        for i, route in enumerate(related, start=1):
            print(f"{i}. Name: {route.name}")
            print(f"   Destination Range: {route.raw.get('dest_range', 'N/A')}")
            print(f"   Priority: {route.raw.get('priority', 'N/A')}")

            next_hop = (
                route.raw.get("next_hop_gateway")
                or route.raw.get("next_hop_ilb")
                or route.raw.get("next_hop_instance")
                or route.raw.get("next_hop_ip")
                or route.raw.get("next_hop_network")
                or route.raw.get("next_hop_vpn_tunnel")
                or "N/A"
            )
            print(f"   Next Hop: {next_hop}")

            tags = route.raw.get("tags", [])
            print(f"   Tags: {', '.join(tags) if tags else 'None'}")
            print("-" * 70)


def show_vpc_connectivity(selected_vpc, creds, project_id):
    print("\n--- Loading Connectivity ---")

    subnets = NetworkOrchestrator.list_resources("subnet", creds, project_id)
    firewalls = NetworkOrchestrator.list_resources("firewall", creds, project_id)
    routes = NetworkOrchestrator.list_resources("route", creds, project_id)
    router_nat = NetworkingInventory.list_routers_and_nats(creds, project_id)

    related_subnets = [s for s in subnets if s.raw.get("network") == selected_vpc.name]
    related_firewalls = [
        f for f in firewalls if f.raw.get("network") == selected_vpc.name
    ]
    related_routes = [r for r in routes if r.raw.get("network") == selected_vpc.name]
    related_routers = [
        r
        for r in router_nat
        if r.resource_type == "cloud_router"
        and r.raw.get("network") == selected_vpc.name
    ]
    related_nats = [n for n in router_nat if n.resource_type == "cloud_nat"]

    print(f"\nConnectivity Summary for {selected_vpc.name}")
    print("=" * 70)
    print(f"Attached Subnets:        {len(related_subnets)}")
    print(f"Attached Firewall Rules: {len(related_firewalls)}")
    print(f"Attached Routes:         {len(related_routes)}")
    print(f"Cloud Routers:           {len(related_routers)}")
    print(f"Cloud NATs:              {len(related_nats)}")

    if related_routers:
        print("\nRouters:")
        for r in related_routers:
            print(f" - {r.name} ({r.location})")

    if related_nats:
        print("\nNAT Configurations:")
        for n in related_nats:
            print(
                f" - {n.name} (Router: {n.raw.get('router', 'N/A')}, Region: {n.location})"
            )

    print("=" * 70)


def auto_enable_flow_logs(subnet_name, region, project_id):  # 🟢 ADDED project_id here
    if not region or region == "N/A":
        region = input(
            f"Enter the region for {subnet_name} (e.g., us-central1): "
        ).strip()

    print("\n" + "-" * 60)
    print(f"🔧 AUTO-FIX: Enabling VPC Flow Logs for {subnet_name} in {region}...")

    gcloud_exec = "gcloud.cmd" if os.name == "nt" else "gcloud"

    cmd = [
        gcloud_exec,
        "compute",
        "networks",
        "subnets",
        "update",
        subnet_name,
        "--region",
        region,
        "--project",
        project_id,  # 🟢 THE FIX: Tell gcloud exactly which project to use!
        "--enable-flow-logs",
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, shell=True)
        print("✅ SUCCESS: VPC Flow Logs are now enabled!")
        print(
            "💡 NOTE: It takes 5-10 minutes for GCP to generate enough traffic to create the metric. Alerts might fail until then."
        )
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to enable flow logs via CLI.\nError details: {e.stderr}")
    except FileNotFoundError:
        print(f"❌ ERROR: Could not find the Google Cloud SDK.")
        print(
            "Please ensure 'gcloud' is installed and added to your Windows system PATH."
        )


def configure_custom_firewall_metric(creds, project_id, firewall_name):
    print(f"\n--- Firewall Alert Configuration ({firewall_name}) ---")
    print(
        "⚠️ NOTE: Firewall alerts require 'Firewall Rules Logging' to be enabled in GCP."
    )
    alert_name = input("\nEnter alert name:\n> ").strip() or f"{firewall_name}-alert"

    print("\nEnter metric type:")
    print("1: Rule Hit Count (Overall Traffic)")
    print("2: Allowed Traffic Count (Spike Detection)")
    print("3: Denied Traffic Count (Brute Force Detection)")
    m_type_choice = input("> ").strip()

    # Mapping to exact GCP Firewall Log Metrics
    metric_map = {
        "1": {
            "label": "Rule Hit Count",
            "gcp_metric": f'metric.type="logging.googleapis.com/log_entry_count" AND resource.type="gce_subnetwork" AND metric.labels.firewall_name="{firewall_name}"',
            "unit": "hits/s",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">=",
            "resource_type": "gce_subnetwork",
        },
        "2": {
            "label": "Allowed Traffic Count",
            "gcp_metric": f'metric.type="logging.googleapis.com/log_entry_count" AND resource.type="gce_subnetwork" AND metric.labels.firewall_name="{firewall_name}" AND metric.labels.disposition="ALLOWED"',
            "unit": "hits/s",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">",
            "resource_type": "gce_subnetwork",
        },
        "3": {
            "label": "Denied Traffic Count",
            "gcp_metric": f'metric.type="logging.googleapis.com/log_entry_count" AND resource.type="gce_subnetwork" AND metric.labels.firewall_name="{firewall_name}" AND metric.labels.disposition="DENIED"',
            "unit": "hits/s",
            "transform": "identity",
            "aligner": "ALIGN_RATE",
            "default_operator": ">",
            "resource_type": "gce_subnetwork",
        },
    }

    selected = metric_map.get(m_type_choice, metric_map["1"])

    print("\nSelect operator:\n1: >\n2: <\n3: >=\n4: <=")
    op_choice = input("> ").strip()
    op_map = {"1": ">", "2": "<", "3": ">=", "4": "<="}
    operator = op_map.get(op_choice, selected["default_operator"])

    threshold_val = float(
        input(f"\nEnter threshold value ({selected['unit']}):\n> ").strip() or "100"
    )
    eval_window = int(
        input("\nEnter evaluation window (seconds, min 60):\n> ").strip() or "60"
    )
    align_period = int(
        input("\nEnter alignment period (seconds, min 60):\n> ").strip() or "60"
    )

    if align_period > eval_window:
        align_period = eval_window

    return {
        "alert_name": alert_name,
        "label": selected["label"],
        "unit": selected["unit"],
        "gcp_metric": selected["gcp_metric"],
        "operator": operator,
        "threshold_value": threshold_val,
        "duration_seconds": eval_window,
        "alignment_period": align_period,
        "transform": selected["transform"],
        "aligner": selected["aligner"],
        "resource_type": selected["resource_type"],
    }


def get_tabs_for_category(cat_key):
    if cat_key == "vm":
        return ["Overview", "CPU", "Memory", "Disk", "Network", "Processes", "Alerts"]
    elif cat_key == "gke":
        return ["Overview", "Nodes", "Workloads", "Networking", "Alerts"]
    elif cat_key == "database":
        return ["Overview", "Configuration", "Performance", "Connections", "Alerts"]
    elif cat_key == "firewall":
        return [
            "Overview",
            "Configuration",
            "Traffic Metrics",
            "Security Insights",
            "Alerts",
        ]
    elif cat_key == "subnet":
        return ["Overview", "Configuration", "Metrics", "Alerts"]
    elif cat_key == "route":
        return ["Overview", "Configuration", "Alerts"]
    elif cat_key == "load_balancer":
        return ["Overview", "Configuration", "Backend Health", "Traffic", "Alerts"]
    elif cat_key == "router":
        return ["Overview", "Configuration", "BGP Status", "Alerts"]
    elif cat_key == "nat":
        return ["Overview", "Configuration", "Port Usage", "Alerts"]
    else:
        return ["Overview", "Configuration", "Alerts"]


def main() -> int:
    args = parse_args()

    conn = GcpConnectionInput(
        service_account_file=args.sa_file,
        service_account_json=args.sa_json,
        service_account_b64=args.sa_b64,
        project_id=args.project,
    )

    creds, project_hint = AuthManager.load_credentials(conn)

    # 1. Test Project Discovery
    if args.list_projects:
        projects = ProjectDiscovery.list_accessible_projects(creds)
        print(json.dumps({"projects": projects}, indent=2))
        return 0

    project_id = conn.project_id or project_hint

    # If user didn't pass --project, ask interactively
    if not project_id:
        try:
            project_id = choose_project_interactive(creds)
        except Exception as e:
            print(json.dumps({"error": str(e)}, indent=2))
            return 2

    print(f"\n✅ Selected project: {project_id}")

    # 2. Test VM CPU Metrics (CLI Flags)
    if args.test_vm_cpu:
        print(f"\n📈 Fetching CPU metrics for VM: {args.test_vm_cpu}...")
        try:
            metrics_data = VmMetricsOrchestrator.get_cpu_utilization(
                creds, project_id, args.test_vm_cpu
            )
            print(json.dumps(metrics_data, indent=2))
        except Exception as e:
            print(f"Failed to fetch CPU metrics: {e}")
        return 0

    # 3. Test VM Logs (CLI Flags)
    if args.test_vm_logs:
        print(f"\n📜 Fetching last 50 logs for VM: {args.test_vm_logs}...")
        try:
            logs_data = VmLogsOrchestrator.get_recent_logs(
                creds, project_id, args.test_vm_logs
            )
            print(json.dumps(logs_data, indent=2))
        except Exception as e:
            print(f"Failed to fetch logs: {e}")
        return 0

    # 4. Default behavior: Fetch Full Inventory (Happens once)
    inv = InventoryOrchestrator.fetch_project_inventory(creds, project_id)

    if inv.errors:
        print(
            json.dumps({"project_id": inv.project_id, "errors": inv.errors}, indent=2)
        )

    # ====================================================================
    # 🔁 LEVEL 1: MAIN SERVICE LOOP
    # ====================================================================
    while True:
        print("\n" + "=" * 70)
        print("🌍 SERVICE EXPLORER")
        print("=" * 70)
        print("Which service would you like to explore?")
        print("1: VM | 2: GKE | 3: Database | 4: Networking")

        service_map = {
            "1": "vm",
            "vm": "vm",
            "2": "gke",
            "gke": "gke",
            "3": "database",
            "database": "database",
            "4": "networking",
            "networking": "networking",
        }

        choice = (
            input("\nEnter the service name or number (or press Enter to quit): ")
            .strip()
            .lower()
        )
        if not choice:
            print("Exiting Lens Explorer. Goodbye!")
            return 0  # Only exit the whole script here!

        selected_service = service_map.get(choice)
        if not selected_service:
            print("Invalid choice. Try again.")
            continue

        # ====================================================================
        # 🟢 PATH 1: NETWORKING EXPLORER
        # ====================================================================
        if selected_service == "networking":
            while True:  # 🔁 LEVEL 2: NETWORKING CATEGORY LOOP
                categories = {
                    "1": ("vpc", "VPC Networks"),
                    "2": ("subnet", "Subnets"),
                    "3": ("firewall", "Firewall Rules"),
                    "4": ("route", "Routes"),
                    "5": ("lb", "Load Balancers"),
                    "6": ("router", "Cloud Routers"),
                    "7": ("nat", "Cloud NAT"),
                }
                print("\n--- Networking Categories ---")
                for k, v in categories.items():
                    print(f"{k}: {v[1]}")

                cat_choice = input(
                    "\nEnter category number (or press Enter to go back to Services): "
                ).strip()
                if not cat_choice:
                    break  # Break out of Category Loop -> Goes back to Service Explorer

                cat_key, cat_label = categories.get(cat_choice, (None, None))
                if not cat_key:
                    print("Invalid category. Try again.")
                    continue

                print(f"\nFetching {cat_label}...")
                resources = NetworkOrchestrator.list_resources(
                    cat_key, creds, project_id
                )

                if not resources:
                    print(f"No {cat_label} found.")
                    input("Press Enter to continue...")
                    continue
                while True:  # 🔁 LEVEL 3: NETWORKING RESOURCE LOOP
                    print(f"\nFound {len(resources)} {cat_label}:")

                    if cat_key == "subnet":
                        print("\n" + "=" * 100)
                        print(
                            f"{'No.':<5} {'Subnet Name':<32} {'Region':<18} {'VPC':<22} {'CIDR':<18}"
                        )
                        print("=" * 100)

                        for i, res in enumerate(resources, start=1):
                            print(
                                f"{i:<5} "
                                f"{res.name[:30]:<32} "
                                f"{(res.location or 'N/A')[:16]:<18} "
                                f"{str(res.raw.get('network', 'N/A'))[:20]:<22} "
                                f"{str(res.raw.get('ip_cidr_range', 'N/A')):<18}"
                            )

                        print("=" * 100)

                    elif cat_key == "firewall":
                        for i, res in enumerate(resources, start=1):
                            print(f"{i}. {res.name}")
                            print(f"   Network: {res.raw.get('network', 'N/A')}")
                            print(f"   Direction: {res.raw.get('direction', 'N/A')}")
                            print(f"   Priority: {res.raw.get('priority', 'N/A')}")
                            print("-" * 60)

                    elif cat_key == "route":
                        for i, res in enumerate(resources, start=1):
                            print(f"{i}. {res.name}")
                            print(f"   Network: {res.raw.get('network', 'N/A')}")
                            print(
                                f"   Destination Range: {res.raw.get('dest_range', 'N/A')}"
                            )
                            print(f"   Priority: {res.raw.get('priority', 'N/A')}")
                            print("-" * 60)

                    elif cat_key == "router":
                        for i, res in enumerate(resources, start=1):
                            print(f"{i}. {res.name}")
                            print(f"   Region: {res.location or 'N/A'}")
                            print(f"   Network: {res.raw.get('network', 'N/A')}")
                            print(f"   ASN: {res.raw.get('bgp', {}).get('asn', 'N/A')}")
                            print("-" * 60)

                    elif cat_key == "nat":
                        for i, res in enumerate(resources, start=1):
                            print(f"{i}. {res.name}")
                            print(f"   Region: {res.location or 'N/A'}")
                            print(f"   Router: {res.raw.get('router', 'N/A')}")
                            print(
                                f"   NAT IP Allocation: {res.raw.get('nat_ip_allocate_option', 'N/A')}"
                            )
                            print("-" * 60)

                    else:
                        for i, res in enumerate(resources, start=1):
                            print(f"{i}. {res.name}")

                    raw = input(
                        f"\nEnter {cat_label} number to explore (or press Enter to go back): "
                    ).strip()

                    if not raw:
                        break  # Back to Categories

                    if not raw.isdigit() or not (0 <= int(raw) - 1 < len(resources)):
                        print("Invalid resource number.")
                        continue

                    idx = int(raw) - 1
                    selected_res = resources[idx]

                    while True:  # 🔁 LEVEL 4: NETWORKING TABS LOOP
                        tabs = NetworkingCatalog.get_tabs(cat_key)
                        print(f"\nSelect a tab to open for {selected_res.name}:")
                        tab_map = {str(i + 1): t for i, t in enumerate(tabs)}
                        for k, v in tab_map.items():
                            print(f"{k}: {v}")

                        tab_choice = input(
                            "\nEnter tab number (or press Enter to go back): "
                        ).strip()
                        if not tab_choice:
                            break  # Break out of Tabs Loop -> Goes back to Resource List

                        selected_tab = tab_map.get(tab_choice)
                        if not selected_tab:
                            print("Invalid tab choice.")
                            continue
                        print(f"DEBUG cat_key = {cat_key}")
                        print(f"DEBUG selected_tab = {selected_tab}")

                        # =========================
                        # SUBNET-SPECIFIC HANDLERS
                        # =========================
                        if cat_key == "subnet" and selected_tab == "Overview":
                            print("\n" + "=" * 60)
                            print(f"🌐 SUBNET OVERVIEW: {selected_res.name}")
                            print("=" * 60)
                            print(f"Project ID:              {project_id}")
                            print(
                                f"Region:                  {selected_res.location or 'N/A'}"
                            )
                            print(
                                f"VPC Network:             {selected_res.raw.get('network', 'N/A')}"
                            )
                            print(
                                f"CIDR Range:              {selected_res.raw.get('ip_cidr_range', 'N/A')}"
                            )
                            print(
                                f"Private Google Access:   {selected_res.raw.get('private_ip_google_access', 'N/A')}"
                            )
                            print("=" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        elif cat_key == "subnet" and selected_tab == "Configuration":
                            print("\n" + "=" * 60)
                            print(f"⚙️ SUBNET CONFIGURATION: {selected_res.name}")
                            print("=" * 60)
                            print(json.dumps(selected_res.raw, indent=2))
                            print("=" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        elif cat_key == "subnet" and selected_tab == "Metrics":
                            print("\n" + "=" * 60)
                            print(f"📊 SUBNET METRICS: {selected_res.name}")
                            print("=" * 60)
                            print("Fetching recent Network Traffic for this Subnet...")

                            metrics_data = (
                                NetworkMetricsOrchestrator.get_subnet_metrics(
                                    creds, project_id, selected_res.name
                                )
                            )
                            print(f"\n{metrics_data['message']}")

                            # 🟢 CHANGED: Offer to auto-fix if no data is found, BUT remember if we just did it!
                            if metrics_data["status"] == "empty":
                                # Check if we already tagged this subnet in the current session
                                if getattr(
                                    selected_res, "_flow_logs_just_enabled", False
                                ):
                                    print(
                                        "\n⏳ Flow Logs were just enabled for this subnet! "
                                        "Please wait 5-10 minutes for Google Cloud to process the traffic data."
                                    )
                                else:
                                    print(
                                        "\n💡 It looks like VPC Flow Logs are disabled or haven't generated data yet."
                                    )
                                    enable_choice = (
                                        input(
                                            f"Do you want to automatically enable Flow Logs for {selected_res.name} now? (y/n): "
                                        )
                                        .strip()
                                        .lower()
                                    )
                                    if enable_choice == "y":
                                        auto_enable_flow_logs(
                                            selected_res.name,
                                            selected_res.location,
                                            project_id,
                                        )
                                        # 🧠 Give the script a memory! Tag this subnet so we don't ask again.
                                        selected_res._flow_logs_just_enabled = True

                            print("=" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        elif cat_key == "subnet" and selected_tab == "Alerts":
                            print("\n" + "=" * 60)
                            print(f"🚨 ALERTS FOR SUBNET: {selected_res.name}")
                            print("=" * 60)
                            print("1: View Existing Alerts")
                            print("2: Create New Alert Policy")

                            alert_choice = input(
                                "\nEnter choice (or press Enter to go back): "
                            ).strip()
                            if not alert_choice:
                                continue

                            if alert_choice == "1":
                                print(
                                    f"\n🔍 Searching GCP for Alerts attached to {selected_res.name}..."
                                )
                                # We can reuse the network alert fetcher since it searches by name
                                alerts = (
                                    NetworkAlertPolicyOrchestrator.list_network_alerts(
                                        creds, project_id, selected_res.name
                                    )
                                )
                                if not alerts:
                                    print("No alerts configured for this subnet.")
                                else:
                                    print(json.dumps(alerts, indent=2))

                            elif alert_choice == "2":
                                # 🟢 NEW: Ask to enable flow logs BEFORE creating the alert
                                print(
                                    "\n⚠️  GCP requires VPC Flow Logs to be enabled BEFORE you can create a traffic alert."
                                )
                                enable_choice = (
                                    input(
                                        f"Do you want to verify/enable Flow Logs for {selected_res.name} now? (y/n): "
                                    )
                                    .strip()
                                    .lower()
                                )

                                if enable_choice == "y":
                                    auto_enable_flow_logs(
                                        selected_res.name,
                                        selected_res.location,
                                        project_id,
                                    )
                                    input(
                                        "\nPress Enter to continue to Alert Configuration..."
                                    )

                                # Proceed with existing alert configuration...
                                custom_data = configure_custom_subnet_metric(
                                    creds, project_id, selected_res.name
                                )

                                if custom_data:
                                    print("\n🚨 SUMMARY: ALERT TO BE CREATED IN GCP")
                                    print(f"Alert Name: {custom_data['alert_name']}")
                                    print(f"Metric:     {custom_data['label']}")
                                    print(
                                        f"Condition:  {custom_data['operator']} {custom_data['threshold_value']} {custom_data['unit']}"
                                    )

                                    confirm = (
                                        input(
                                            "\nPush this configuration to Google Cloud now? (y/n): "
                                        )
                                        .strip()
                                        .lower()
                                    )
                                    if confirm == "y":
                                        try:
                                            NetworkAlertPolicyOrchestrator.create_network_alert_policy(
                                                credentials=creds,
                                                project_id=project_id,
                                                network_name=selected_res.name,
                                                custom_data=custom_data,
                                            )
                                            print(
                                                "\n✅ SUBNET ALERT POLICY CREATED SUCCESSFULLY!"
                                            )
                                        except Exception as e:
                                            print(f"\n❌ Failed to create alert: {e}")
                                    else:
                                        print("Cancelled.")

                        # --- NETWORKING ROUTING LOGIC (4 TABS) ---
                        if selected_tab == "Overview":
                            print("\n" + "=" * 60)
                            print(f"🌍 OVERVIEW: {selected_res.name}")
                            print("=" * 60)
                            print(f"Project ID:          {project_id}")

                            # 🟢 Make Overview dynamic based on what resource we are looking at!
                            if cat_key == "vpc":
                                auto_create = selected_res.raw.get(
                                    "auto_create_subnetworks", "N/A"
                                )
                                routing_mode = selected_res.raw.get(
                                    "routing_config", {}
                                ).get("routing_mode", "N/A")
                                print(f"Auto-Create Subnets: {auto_create}")
                                print(f"Routing Mode:        {routing_mode}")

                            elif cat_key == "firewall":
                                # 🟢 UPGRADED FIREWALL OVERVIEW
                                action = (
                                    "ALLOW" if "allowed" in selected_res.raw else "DENY"
                                )
                                log_config = selected_res.raw.get(
                                    "logConfig", {}
                                ) or selected_res.raw.get("log_config", {})
                                logging_enabled = log_config.get("enable", False)

                                print(
                                    f"Network:             {selected_res.raw.get('network', 'N/A')}"
                                )
                                print(
                                    f"Direction:           {selected_res.raw.get('direction', 'N/A')}"
                                )
                                print(f"Action:              {action}")
                                print(
                                    f"Priority:            {selected_res.raw.get('priority', 'N/A')}"
                                )
                                print(
                                    f"Logging Enabled:     {'✅ Yes' if logging_enabled else '❌ No'}"
                                )

                            elif cat_key == "route":
                                print(
                                    f"Network:             {selected_res.raw.get('network', 'N/A')}"
                                )
                                print(
                                    f"Dest Range:          {selected_res.raw.get('dest_range', 'N/A')}"
                                )
                                print(
                                    f"Priority:            {selected_res.raw.get('priority', 'N/A')}"
                                )

                            print("=" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        # 🟢 NEW: SECURITY INSIGHTS TAB (Only triggers for Firewalls!)
                        # 🟢 1. SECURITY INSIGHTS TAB
                        elif (
                            selected_tab == "Security Insights"
                            and cat_key == "firewall"
                        ):
                            print("\n" + "=" * 60)
                            print(f"🛡️ SECURITY INSIGHTS: {selected_res.name}")
                            print("=" * 60)

                            insights = []
                            source_ranges = selected_res.raw.get(
                                "sourceRanges", []
                            ) or selected_res.raw.get("source_ranges", [])
                            log_config = selected_res.raw.get(
                                "logConfig", {}
                            ) or selected_res.raw.get("log_config", {})

                            # Check for 0.0.0.0/0
                            if "0.0.0.0/0" in source_ranges:
                                insights.append(
                                    "🚨 CRITICAL: Rule is open to the entire internet (0.0.0.0/0)!"
                                )
                            else:
                                insights.append("✅ PASS: Rule is not globally open.")

                            # Check Logging Status
                            if not log_config.get("enable", False):
                                insights.append(
                                    "⚠️ WARNING: Firewall Logging is DISABLED. Traffic hits cannot be monitored."
                                )
                            else:
                                insights.append("✅ PASS: Firewall Logging is ENABLED.")

                            # Check Direction & Priority
                            if (
                                selected_res.raw.get("direction") == "EGRESS"
                                and selected_res.raw.get("priority", 1000) == 1000
                            ):
                                insights.append(
                                    "💡 NOTE: This is a standard EGRESS rule. Ensure it doesn't shadow lower-priority denys."
                                )

                            for insight in insights:
                                print(insight)

                            print("=" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        # 🟢 2. TRAFFIC METRICS TAB
                        elif (
                            selected_tab == "Traffic Metrics" and cat_key == "firewall"
                        ):
                            print("\n" + "=" * 60)
                            print(f"📊 TRAFFIC METRICS: {selected_res.name}")
                            print("=" * 60)

                            log_config = selected_res.raw.get(
                                "logConfig", {}
                            ) or selected_res.raw.get("log_config", {})
                            if not log_config.get("enable", False):
                                print(
                                    "⚠️ Firewall Rules Logging is DISABLED for this rule."
                                )
                                print(
                                    "Firewall Insights metrics are generated only for rules with logging enabled."
                                )
                                print(
                                    "\n💡 TIP: Enable Firewall Rules Logging, wait a few minutes, then check again."
                                )
                                print("=" * 60)
                                input("\nPress Enter to return to tabs...")
                                continue

                            print("Select time range:")
                            print("1: Last 1 hour")
                            print("2: Last 24 hours")
                            print("3: Last 7 days")

                            range_choice = input(
                                "\nEnter choice [default: 24h]: "
                            ).strip()
                            lookback_hours = 24
                            if range_choice == "1":
                                lookback_hours = 1
                            elif range_choice == "3":
                                lookback_hours = 168

                            print(
                                f"\nFetching Firewall Insights metrics for last {lookback_hours} hour(s)..."
                            )

                            metrics_response = (
                                NetworkMetricsOrchestrator.get_firewall_metrics(
                                    creds,
                                    project_id,
                                    selected_res.name,
                                    lookback_hours=lookback_hours,
                                )
                            )

                            if metrics_response["status"] == "success":
                                data = metrics_response["data"]
                                print("\n--- Firewall Insights ---")
                                print(
                                    f"1️⃣ Rule Hit Count ({data['lookback_hours']}h): {data['total_hits']}"
                                )

                                if data["last_used_readable"]:
                                    print(
                                        f"2️⃣ Last Used:                  {data['last_used_readable']}"
                                    )
                                else:
                                    print(
                                        "2️⃣ Last Used:                  No timestamp available"
                                    )

                                if data["total_hits"] > 0:
                                    print(
                                        "\n💡 INSIGHT: This rule is actively matching traffic."
                                    )
                                else:
                                    print(
                                        "\n💡 INSIGHT: No hit-count points were returned in this window."
                                    )
                                    print(
                                        "           Treat this as 'no observed recent TCP/UDP hits', not a guaranteed unused rule."
                                    )

                            elif metrics_response["status"] == "no_data":
                                print(metrics_response["message"])
                                print("\n--- Firewall Insights ---")
                                print(
                                    "1️⃣ Rule Hit Count:             No metric points returned"
                                )
                                print(
                                    "2️⃣ Last Used:                  No timestamp available"
                                )
                                print(
                                    "\n💡 INSIGHT: This does NOT conclusively prove the rule is unused."
                                )
                            else:
                                print(metrics_response["message"])

                            print("=" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue
                        # 🟢 NEW: Add the missing Configuration handler for non-subnet resources!
                        elif selected_tab == "Configuration":
                            print("\n" + "=" * 60)
                            print(f"⚙️ CONFIGURATION: {selected_res.name}")
                            print("=" * 60)
                            print(json.dumps(selected_res.raw, indent=2))
                            print("=" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        elif selected_tab == "Subnets":
                            print("\n--- Loading Subnets ---")
                            all_subnets = NetworkOrchestrator.list_resources(
                                "subnet", creds, project_id
                            )
                            attached_subnets = [
                                s
                                for s in all_subnets
                                if s.raw.get("network") == selected_res.name
                            ]

                            print(f"\nViewing Subnets related to {selected_res.name}")
                            print("=" * 60)
                            if not attached_subnets:
                                print("No subnets found for this VPC.")
                            else:
                                for i, s in enumerate(attached_subnets, start=1):
                                    print(f"{i}. {s.name}")
                                    print(f"   Region: {s.location}")
                                    print(
                                        f"   CIDR: {s.raw.get('ip_cidr_range', 'N/A')}"
                                    )
                                    print(
                                        f"   Private Google Access: {s.raw.get('private_ip_google_access', 'N/A')}"
                                    )
                                    print("-" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        elif selected_tab == "Firewall Rules":
                            print("\n--- Loading Firewall Rules ---")
                            all_firewalls = NetworkOrchestrator.list_resources(
                                "firewall", creds, project_id
                            )
                            attached_firewalls = [
                                f
                                for f in all_firewalls
                                if f.raw.get("network") == selected_res.name
                            ]

                            print(f"\nViewing Firewalls related to {selected_res.name}")
                            print("=" * 60)
                            if not attached_firewalls:
                                print("No firewall rules found for this VPC.")
                            else:
                                for i, f in enumerate(attached_firewalls, start=1):
                                    print(f"{i}. {f.name}")
                                    print(
                                        f"   Direction: {f.raw.get('direction', 'N/A')}"
                                    )
                                    print(
                                        f"   Priority: {f.raw.get('priority', 'N/A')}"
                                    )
                                    print(
                                        f"   Source Ranges: {', '.join(f.raw.get('source_ranges', [])) or 'N/A'}"
                                    )
                                    print(
                                        f"   Target Tags: {', '.join(f.raw.get('target_tags', [])) or 'N/A'}"
                                    )
                                    print("-" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        elif selected_tab == "Routes":
                            print("\n--- Loading Routes ---")
                            all_routes = NetworkOrchestrator.list_resources(
                                "route", creds, project_id
                            )
                            attached_routes = [
                                r
                                for r in all_routes
                                if r.raw.get("network") == selected_res.name
                            ]

                            print(f"\nViewing Routes related to {selected_res.name}")
                            print("=" * 60)
                            if not attached_routes:
                                print("No routes found for this VPC.")
                            else:
                                for i, r in enumerate(attached_routes, start=1):
                                    print(f"{i}. {r.name}")
                                    print(
                                        f"   Destination Range: {r.raw.get('dest_range', 'N/A')}"
                                    )
                                    print(
                                        f"   Priority: {r.raw.get('priority', 'N/A')}"
                                    )
                                    next_hop = (
                                        r.raw.get("next_hop_gateway")
                                        or r.raw.get("next_hop_ilb")
                                        or r.raw.get("next_hop_instance")
                                        or r.raw.get("next_hop_ip")
                                        or r.raw.get("next_hop_network")
                                        or r.raw.get("next_hop_vpn_tunnel")
                                        or "N/A"
                                    )
                                    print(f"   Next Hop: {next_hop}")
                                    print("-" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        elif selected_tab == "Connectivity":
                            print("\n--- Loading Connectivity ---")
                            all_subnets = NetworkOrchestrator.list_resources(
                                "subnet", creds, project_id
                            )
                            all_firewalls = NetworkOrchestrator.list_resources(
                                "firewall", creds, project_id
                            )
                            all_routes = NetworkOrchestrator.list_resources(
                                "route", creds, project_id
                            )

                            attached_subnets = [
                                s
                                for s in all_subnets
                                if s.raw.get("network") == selected_res.name
                            ]
                            attached_firewalls = [
                                f
                                for f in all_firewalls
                                if f.raw.get("network") == selected_res.name
                            ]
                            attached_routes = [
                                r
                                for r in all_routes
                                if r.raw.get("network") == selected_res.name
                            ]

                            print(f"\nConnectivity Summary for {selected_res.name}")
                            print("=" * 60)
                            print(f"Attached Subnets:        {len(attached_subnets)}")
                            print(f"Attached Firewall Rules: {len(attached_firewalls)}")
                            print(f"Attached Routes:         {len(attached_routes)}")
                            print("=" * 60)
                            input("\nPress Enter to return to tabs...")
                            continue

                        elif selected_tab == "Alerts":
                            print("\n" + "=" * 60)
                            print(f"🚨 ALERTS FOR: {selected_res.name}")
                            print("=" * 60)
                            print("1: View Existing Alerts")
                            print("2: Create New Alert Policy")

                            alert_choice = input(
                                "\nEnter choice (or press Enter to go back): "
                            ).strip()
                            if not alert_choice:
                                continue

                            if alert_choice == "1":
                                print(
                                    f"\n🔍 Searching GCP for Alerts attached to {selected_res.name}..."
                                )

                                # 🟢 CHANGED: We now use our dedicated Network alert finder!
                                alerts = (
                                    NetworkAlertPolicyOrchestrator.list_network_alerts(
                                        creds, project_id, selected_res.name
                                    )
                                )

                                if not alerts:
                                    print(
                                        "No alerts configured for this network resource."
                                    )
                                else:
                                    print(json.dumps(alerts, indent=2))

                            elif alert_choice == "2":
                                print(
                                    f"\n⚙️ Configure Network Alert for {selected_res.name}"
                                )
                                custom_data = configure_custom_network_metric(
                                    creds, project_id, selected_res.name
                                )

                                if custom_data:
                                    print("\n🚨 SUMMARY: ALERT TO BE CREATED IN GCP")
                                    print(f"Alert Name: {custom_data['alert_name']}")
                                    print(f"Metric:     {custom_data['label']}")
                                    print(
                                        f"Condition:  {custom_data['operator']} {custom_data['threshold_value']} {custom_data['unit']}"
                                    )

                                    confirm = (
                                        input(
                                            "\nPush this configuration to Google Cloud now? (y/n): "
                                        )
                                        .strip()
                                        .lower()
                                    )
                                    if confirm == "y":
                                        try:
                                            # 🟢 USES THE CORRECT NETWORK ORCHESTRATOR
                                            NetworkAlertPolicyOrchestrator.create_network_alert_policy(
                                                credentials=creds,
                                                project_id=project_id,
                                                network_name=selected_res.name,
                                                custom_data=custom_data,
                                            )
                                            print(
                                                "\n✅ ALERT POLICY CREATED SUCCESSFULLY!"
                                            )
                                        except Exception as e:
                                            print(f"\n❌ Failed to create alert: {e}")
                                    else:
                                        print("Cancelled.")
                            input("\nPress Enter to return to tabs...")
                            continue

                        # Fallback for Metrics/Logs if you explore Subnets or NATs
                        elif selected_tab in ["Metrics", "Logs"]:
                            print(
                                f"\n🚧 {selected_tab} for {selected_res.name} coming soon!"
                            )
                            input("\nPress Enter to return to tabs...")
                            continue

        # ====================================================================
        # 🟢 PATH 2: VM / GKE / DATABASE EXPLORER
        # ====================================================================
        elif selected_service in inv.services:
            while True:  # 🔁 LEVEL 2: VM RESOURCE LOOP
                resources = inv.services[selected_service]
                print(
                    f"\nFound {len(resources)} resources in {selected_service.upper()}:"
                )

                for i, res in enumerate(resources, start=1):
                    if selected_service == "vm":
                        status_display = status_dot(res.status)
                        print(
                            f"{i}. {res.name} ({status_display}, zone: {res.location})"
                        )
                    else:
                        print(f"{i}. {res.name} (status: {res.status})")

                raw = input(
                    "\nEnter the resource number to explore (or press Enter to go back to Services): "
                ).strip()
                if not raw:
                    break  # Break out to Service Explorer

                if not raw.isdigit() or not (0 <= int(raw) - 1 < len(resources)):
                    print("Invalid resource number.")
                    continue

                idx = int(raw) - 1
                selected_res = resources[idx]

                while True:  # 🔁 LEVEL 3: VM TABS LOOP
                    tabs = ObservabilityCatalog.get_tabs(selected_res)
                    print(f"\nSelect a tab to open for {selected_res.name}:")
                    tab_map = {str(i + 1): t for i, t in enumerate(tabs)}
                    for k, v in tab_map.items():
                        print(f"{k}: {v}")

                    tab_choice = input(
                        "\nEnter tab number (or press Enter to go back): "
                    ).strip()
                    if not tab_choice:
                        break  # Break out to VM Resource List

                    selected_tab = tab_map.get(tab_choice)
                    if not selected_tab:
                        print("Invalid tab choice.")
                        continue

                    # --- ROUTING LOGIC ---
                    if selected_tab == "Logs":
                        print(f"\n📜 Fetching logs for: {selected_res.name}...")
                        formatted_logs = VmLogsOrchestrator.get_recent_logs(
                            creds, project_id, str(selected_res.id), limit=10
                        )
                        print(f"\n📜 Recent Logs for VM: {selected_res.name}")
                        print("-" * 50)
                        for i, log in enumerate(formatted_logs, start=1):
                            ts = log["timestamp"].replace("T", " ")[:19]
                            print(
                                f"{i}. [{ts}] [{log['severity']}] [{log['log_name']}]"
                            )
                            print(f"   {log['message']}\n")
                        print("-" * 50)
                        input("\nPress Enter to return to tabs...")
                        continue

                    elif selected_tab == "Events":
                        print(
                            f"\n⚡ Fetching recent System Events for VM: {selected_res.name}..."
                        )
                        events = VmSystemOrchestrator.get_audit_events(
                            creds, project_id, str(selected_res.id)
                        )
                        if not events:
                            print("No recent system events found in the last 24 hours.")
                        else:
                            print(json.dumps(events, indent=2))
                        input("\nPress Enter to return to tabs...")
                        continue

                    elif selected_tab == "Alerts":
                        print(
                            f"\n🔍 Searching GCP for Alerts attached to VM: {selected_res.name}..."
                        )
                        alerts = VmSystemOrchestrator.list_vm_alerts(
                            creds, project_id, str(selected_res.id)
                        )
                        if not alerts:
                            print(
                                "No❌❌ alert policies are currently configured for this VM.\nTip: Navigate to CPU, Memory, Disk, or Network tabs to create an ALERT POLICY🚨🚨."
                            )
                        else:
                            print(json.dumps(alerts, indent=2))
                        input("\nPress Enter to return to tabs...")
                        continue

                    # --- 🚨 THE ALERT CONFIGURATOR FLOW 🚨 ---
                    print(f"\n⚙️ Configure Alert Policy for VM: {selected_res.name}")

                    metric_key = choose_metric_from_catalog_interactive(selected_tab)
                    if not metric_key:
                        continue  # Goes back to Tab Menu!

                    metric_cfg = VmMonitoringCatalog.get_metric_config(metric_key)

                    if metric_key == "custom_cpu":
                        custom_data = configure_custom_cpu_metric()
                    elif metric_key == "custom_memory":
                        custom_data = configure_custom_memory_metric()
                    elif metric_key == "custom_disk":
                        custom_data = configure_custom_disk_metric()
                    elif metric_key == "custom_network":
                        custom_data = configure_custom_network_metric()
                    elif metric_key == "custom_process":
                        custom_data = configure_custom_process_metric()
                    else:
                        operator = choose_operator_interactive()
                        if not operator:
                            continue
                        threshold_value = choose_threshold_interactive(metric_key)
                        if threshold_value is None:
                            continue
                        duration_seconds = choose_duration_interactive()
                        if duration_seconds is None:
                            continue
                        custom_data = None

                    if custom_data:
                        metric_cfg.update(
                            {
                                "alert_name": custom_data["alert_name"],
                                "label": custom_data["label"],
                                "unit": custom_data["unit"],
                                "gcp_metric": custom_data["gcp_metric"],
                                "transform": custom_data["transform"],
                                "alignment_period": custom_data["alignment_period"],
                            }
                        )
                        if "aligner" in custom_data:
                            metric_cfg["aligner"] = custom_data["aligner"]
                        if "cross_series_reducer" in custom_data:
                            metric_cfg["cross_series_reducer"] = custom_data[
                                "cross_series_reducer"
                            ]

                        # 🟢 FIX: Moved these INSIDE the if-block!
                        operator = custom_data["operator"]
                        threshold_value = custom_data["threshold_value"]
                        duration_seconds = custom_data["duration_seconds"]

                    # --- 🚨 FINAL SUMMARY AND PUSH TO GCP 🚨 ---
                    print("\n" + "=" * 50)
                    print("🚨 SUMMARY: ALERT TO BE CREATED IN GCP")
                    print("=" * 50)
                    print(f"Resource: VM {selected_res.name}")
                    alert_name_display = metric_cfg.get(
                        "alert_name", metric_cfg["label"]
                    )
                    print(f"Alert Name: {alert_name_display}")
                    print(f"Metric:   {metric_cfg['label']}")
                    unit_str = f" {metric_cfg['unit']}" if metric_cfg["unit"] else ""
                    print(f"Condition: {operator} {threshold_value}{unit_str}")
                    print(f"Duration: {duration_seconds} seconds")
                    print("=" * 50)

                    confirm = (
                        input("\nPush this configuration to Google Cloud now? (y/n): ")
                        .strip()
                        .lower()
                    )

                    if confirm == "y":
                        final_policy_name = metric_cfg.get(
                            "alert_name",
                            f"Lens Auto-Alert | {selected_res.name} | {metric_cfg['label']}",
                        )
                        print("\nPushing to Cloud Monitoring API...")
                        try:
                            VmAlertPolicyOrchestrator.create_vm_alert_policy(
                                credentials=creds,
                                project_id=project_id,
                                instance_ids=[str(selected_res.id)],
                                instance_names=[selected_res.name],
                                metric_key=metric_key,
                                threshold_value=threshold_value,
                                operator=operator,
                                duration_seconds=duration_seconds,
                                policy_display_name=final_policy_name,
                            )
                            print("\n✅ ALERT POLICY CREATED SUCCESSFULLY!")
                        except Exception as e:
                            print(f"\n❌ Failed to create alert policy: {e}")
                    else:
                        print("\nOperation cancelled.")

                    input("\nPress Enter to return to tabs...")

        else:
            print("Invalid choice. Try again.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
