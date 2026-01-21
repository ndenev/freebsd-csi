#!/usr/bin/env python3
"""
UCL config helper for ctld-agent.

This script manages the CSI-controlled targets in /etc/ctl.ucl while preserving
user-managed targets. It uses the Python UCL library for proper parsing and
serialization.

Usage:
    ctl-config-helper.py add-iscsi <iqn> <auth-group> <portal-group> <lun-id> <device-path>
    ctl-config-helper.py add-nvmeof <nqn> <auth-group> <transport-group> <ns-id> <device-path>
    ctl-config-helper.py remove <target-name>
    ctl-config-helper.py list-csi
    ctl-config-helper.py sync <json-file>  # Sync from JSON file with all CSI targets
"""

import sys
import json
import ucl
import locket
import argparse

DEFAULT_CONFIG_PATH = "/etc/ctl.ucl"
LOCK_PATH = "/tmp/ctl-config-helper.lock"

# Global config path (set by --config argument)
CONFIG_PATH = DEFAULT_CONFIG_PATH

# CSI target prefixes - used to identify CSI-managed targets
CSI_ISCSI_PREFIX = "iqn.2024-01.org.freebsd.csi:"
CSI_NVMEOF_PREFIX = "nqn.2024-01.org.freebsd.csi:"


def load_config():
    """Load the UCL config file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            return ucl.load(f.read())
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        sys.exit(1)


def save_config(config):
    """Save the config back to the UCL file."""
    try:
        with open(CONFIG_PATH, 'w') as f:
            f.write(ucl.dump(config))
    except Exception as e:
        print(f"Error saving config: {e}", file=sys.stderr)
        sys.exit(1)


def is_csi_target(name):
    """Check if a target name is CSI-managed."""
    return name.startswith(CSI_ISCSI_PREFIX) or name.startswith(CSI_NVMEOF_PREFIX)


def add_iscsi_target(config, iqn, auth_group, portal_group, lun_id, device_path):
    """Add an iSCSI target to the config."""
    if "target" not in config:
        config["target"] = {}

    config["target"][iqn] = {
        "auth-group": auth_group,
        "portal-group": portal_group,
        "lun": {
            str(lun_id): {
                "path": device_path
            }
        }
    }
    return config


def add_nvmeof_controller(config, nqn, auth_group, transport_group, ns_id, device_path):
    """Add an NVMeoF controller to the config."""
    if "controller" not in config:
        config["controller"] = {}

    config["controller"][nqn] = {
        "auth-group": auth_group,
        "transport-group": transport_group,
        "namespace": {
            str(ns_id): {
                "path": device_path
            }
        }
    }
    return config


def remove_target(config, target_name):
    """Remove a target or controller by name."""
    removed = False

    if "target" in config and target_name in config["target"]:
        del config["target"][target_name]
        removed = True

    if "controller" in config and target_name in config["controller"]:
        del config["controller"][target_name]
        removed = True

    return config, removed


def list_csi_targets(config):
    """List all CSI-managed targets."""
    csi_targets = []

    if "target" in config:
        for name in config["target"]:
            if is_csi_target(name):
                csi_targets.append({"type": "iscsi", "name": name})

    if "controller" in config:
        for name in config["controller"]:
            if is_csi_target(name):
                csi_targets.append({"type": "nvmeof", "name": name})

    return csi_targets


def sync_csi_targets(config, csi_exports):
    """
    Synchronize CSI targets with the provided list.

    csi_exports is a list of dicts with:
    - type: "iscsi" or "nvmeof"
    - name: target/controller name
    - auth_group: auth group name
    - portal_group/transport_group: group name
    - lun_id/ns_id: LUN or namespace ID
    - device_path: path to backing device
    """
    # Remove all existing CSI targets
    if "target" in config:
        config["target"] = {k: v for k, v in config["target"].items() if not is_csi_target(k)}

    if "controller" in config:
        config["controller"] = {k: v for k, v in config["controller"].items() if not is_csi_target(k)}

    # Add new CSI targets
    for export in csi_exports:
        if export["type"] == "iscsi":
            config = add_iscsi_target(
                config,
                export["name"],
                export["auth_group"],
                export["portal_group"],
                export["lun_id"],
                export["device_path"]
            )
        elif export["type"] == "nvmeof":
            config = add_nvmeof_controller(
                config,
                export["name"],
                export["auth_group"],
                export["transport_group"],
                export["ns_id"],
                export["device_path"]
            )

    return config


def main():
    global CONFIG_PATH

    parser = argparse.ArgumentParser(description="UCL config helper for ctld-agent")
    parser.add_argument("--config", "-c", default=DEFAULT_CONFIG_PATH,
                        help=f"Path to UCL config file (default: {DEFAULT_CONFIG_PATH})")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # add-iscsi command
    add_iscsi = subparsers.add_parser("add-iscsi", help="Add an iSCSI target")
    add_iscsi.add_argument("iqn", help="iSCSI Qualified Name")
    add_iscsi.add_argument("auth_group", help="Auth group name")
    add_iscsi.add_argument("portal_group", help="Portal group name")
    add_iscsi.add_argument("lun_id", type=int, help="LUN ID")
    add_iscsi.add_argument("device_path", help="Device path")

    # add-nvmeof command
    add_nvmeof = subparsers.add_parser("add-nvmeof", help="Add an NVMeoF controller")
    add_nvmeof.add_argument("nqn", help="NVMe Qualified Name")
    add_nvmeof.add_argument("auth_group", help="Auth group name")
    add_nvmeof.add_argument("transport_group", help="Transport group name")
    add_nvmeof.add_argument("ns_id", type=int, help="Namespace ID")
    add_nvmeof.add_argument("device_path", help="Device path")

    # remove command
    remove = subparsers.add_parser("remove", help="Remove a target/controller")
    remove.add_argument("target_name", help="Target or controller name to remove")

    # list-csi command
    subparsers.add_parser("list-csi", help="List CSI-managed targets")

    # sync command
    sync = subparsers.add_parser("sync", help="Sync CSI targets from JSON file")
    sync.add_argument("json_file", help="JSON file with CSI exports")

    args = parser.parse_args()

    # Set config path from argument
    CONFIG_PATH = args.config

    # Acquire lock
    lock = locket.lock_file(LOCK_PATH)
    lock.acquire()

    try:
        config = load_config()

        if args.command == "add-iscsi":
            config = add_iscsi_target(
                config, args.iqn, args.auth_group, args.portal_group,
                args.lun_id, args.device_path
            )
            save_config(config)
            print(f"Added iSCSI target: {args.iqn}")

        elif args.command == "add-nvmeof":
            config = add_nvmeof_controller(
                config, args.nqn, args.auth_group, args.transport_group,
                args.ns_id, args.device_path
            )
            save_config(config)
            print(f"Added NVMeoF controller: {args.nqn}")

        elif args.command == "remove":
            config, removed = remove_target(config, args.target_name)
            if removed:
                save_config(config)
                print(f"Removed: {args.target_name}")
            else:
                print(f"Not found: {args.target_name}", file=sys.stderr)
                sys.exit(1)

        elif args.command == "list-csi":
            targets = list_csi_targets(config)
            print(json.dumps(targets, indent=2))

        elif args.command == "sync":
            with open(args.json_file, 'r') as f:
                csi_exports = json.load(f)
            config = sync_csi_targets(config, csi_exports)
            save_config(config)
            print(f"Synced {len(csi_exports)} CSI targets")

    finally:
        lock.release()


if __name__ == "__main__":
    main()
