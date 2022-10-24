import boto3
import botocore
import os
import time
import logging
import argparse
import concurrent.futures

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

MAX_S3_WORKERS = 20


def get_args():
    parser = argparse.ArgumentParser(
        description="S3 Parallel Action",
    )

    # common parser
    parser.add_argument("--region", help="Region", type=str)
    parser.add_argument("--profile", help="AWS Profile", type=str)

    # subparser
    subparsers = parser.add_subparsers(
        help="Desired Action", required=True, dest="action"
    )

    # parent parser args
    parent_parser = argparse.ArgumentParser(add_help=False)

    parent_parser.add_argument(
        "-p",
        "--prefix",
        help="S3 key Prefix",
        default="",
    )
    parent_parser.add_argument("--start-after", help="Start after specified key")
    parent_parser.add_argument("-b", "--bucket", help="Bucket", required=True)
    parent_parser.add_argument("--dry", help="Dry Run", action="store_true")

    # ls parser
    parser_ls = subparsers.add_parser(
        "ls",
        parents=[parent_parser],
        help="List s3 keys versions and optionally DeleteMarker",
    )
    parser_ls.add_argument(
        "--no-versions", help="Do not List Versions", action="store_true"
    )
    parser_ls.add_argument(
        "--delete-marker", help="List DeleteMarkers", action="store_true"
    )
    parser_ls.add_argument(
        "--skip-current-version",
        help="Do not List Current Version",
        action="store_true",
    )

    # rm parser
    parser_rm = subparsers.add_parser(
        "rm",
        parents=[parent_parser],
        help="Remove s3 keys, optionally versions and delete marker",
    )
    parser_rm.add_argument("--versions", help="Remove Versions", action="store_true")
    parser_rm.add_argument(
        "--delete-marker", help="Remove DeleteMarkers", action="store_true"
    )
    parser_rm.add_argument(
        "--skip-current-version",
        help="Do not remove Current Version",
        action="store_true",
    )
    parser_rm.add_argument(
        "-w",
        "--max-s3-workers",
        help=f"Max S3 Workers to use [{MAX_S3_WORKERS}]",
        type=int,
        default=MAX_S3_WORKERS,
    )
    parser_rm.add_argument("--stop-on-error", help="Stop on remove error")

    # cp parser
    parser_cp = subparsers.add_parser(
        "cp",
        parents=[parent_parser],
        help="Copy Key from Bucket to SourceBucket",
    )
    parser_cp.add_argument("--versions", help="Copy Versions", action="store_true")
    parser_cp.add_argument(
        "--delete-marker", help="Copy DeleteMarkers", action="store_true"
    )
    parser_cp.add_argument(
        "--skip-current-version",
        help="Do not copy Current Version",
        action="store_true",
    )
    parser_cp.add_argument(
        "-d", "--dest-bucket", help="Destination Bucket", required=True
    )
    parser_cp.add_argument("--dest-region", help="Destination Region")
    parser_cp.add_argument(
        "-w", "--max-s3-workers", help="Max S3 Workers to use", type=int
    )
    parser_cp.add_argument("--stop-on-error", help="Stop on copy error")

    args = parser.parse_args()
    return args


def human_readable_size(size, decimal_places=2):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0 or unit == "PiB":
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"



def action_cp(args, kwargs, client, key, version, latest):
    resp = client.copy_object(**kwargs)

def action_cp(args, kwargs, client, key, version, latest):
    resp = client.copy_object(**kwargs)

def execute_s3_action(args, kwargs, client, key, version_id, latest, n_tot, s_tot):
    s_tot = human_readable_size(s_tot)

    if args.skip_current_version and latest == True:
        return
    is_latest = "*" if latest else ""

    try:
        if args.action == "ls" and args.no_versions == False and latest == False:
            return
        elif args.action in ["rm", "cp"] and args.versions == False and latest == False:
            return
        elif args.action == "rm":
            kwargs["Key"] = key
            kwargs["VersionId"] = version_id
            resp = client.delete_object(**kwargs)
        elif args.action == "cp":
            kwargs["Key"] = key
            kwargs["CopySource"]["Key"] = key
            kwargs["CopySource"]["VersionId"] = version_id
            resp = client.copy_object(**kwargs)

    except Exception as e:
        status = f"ERROR [{e}]"
    else:
        status = "OK"

    logger.info(f"KEY: {key}, V: {version_id} [{is_latest}], N: {n_tot}, S: {s_tot}, STATUS: {status}")


def get_kwargs_clients(args):
    k_s3_ls = {}
    if args.region:
        k_s3_ls["region_name"] = args.dest_region

    k_s3_act = {}
    k_s3_act_cfg = {}
    if args.action == "cp":
        k_s3_act_cfg["max_pool_connections"] = args.max_s3_workers
        if args.dest_region:
            k_s3_act_cfg["region_name"] = args.dest_region
        k_s3_act["config"] = botocore.client.Config(**k_s3_act_cfg)

    return k_s3_ls, k_s3_act


def get_kwargs_acts(args):
    k_ls = {}
    k_act = {}

    k_ls["Bucket"] == args.bucket
    if args.prefix:
        k_ls["Prefix"] = args.prefix
    if args.start_after:
        k_ls["KeyMarker"] = args.start_after

    k_act["Bucket"] = args.dest_bucket
    if arg.action == "cp":
        k_act["CopySource"] = {
            "Bucket": args.bucket,
        }

    return k_ls, k_act


def run():
    n_tot = s_tot = 0

    args = get_args()

    kwargs_s3_client_ls, kwargs_s3_client_action = get_kwargs_clients(args)
    kwargs_s3_ls, kwargs_s3_action = get_kwargs_acts(args)

    s3_client_ls = boto3.client("s3", **kwargs_s3_client_ls)
    s3_client_action = boto3.client("s3", **kwargs_s3_client_action)

    paginator = s3_client_ls.get_paginator("list_object_versions")
    response_iterator = paginator.paginate(**kwargs_s3_ls)

    for r in response_iterator:

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.max_s3_workers
        ) as executor:
            future_to_stack = {}
            list_objs = r.get("Versions", []).reverse()

            if args.delete_marker:
                list_objs += r.get("DeleteMarkers", []).reverse()

            for p in list_objs:
                s3_key = p.get("Key")
                s3_key_version = p.get("VersionId")
                s3_key_size = p.get("Size")
                s3_key_latest = p.get("IsLatest")
                n_tot += 1
                s_tot += s3_key_size

                ex_sub = executor.submit(
                    execute_s3_action,
                    args,
                    kwargs_s3_action,
                    s3_client_action,
                    s3_key,
                    s3_key_version,
                    s3_key_latest,
                    n_tot,
                    s_tot,
                )
                future_to_stack[ex_sub] = s3_key

            for future in concurrent.futures.as_completed(future_to_stack):
                obj = future_to_stack[future]
                try:
                    s3_status = future.result()
                except Exception as e:
                    break

            if args.stop_on_error:
                for future in future_to_stack:
                    future.cancel()


if __name__ == "__main__":
    run()
