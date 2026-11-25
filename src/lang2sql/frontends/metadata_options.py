"""Shared CLI/MCP input boundary for optional Oracle metadata storage."""

import os


def parse_metadata_args(parser, argv=None):
    parser.add_argument(
        "--metadata-url-env",
        help="Environment variable containing the Oracle metadata URL",
    )
    parser.add_argument(
        "--initialize-metadata",
        action="store_true",
        help="Create metadata tables with the metadata account",
    )
    args = parser.parse_args(argv)
    args.metadata_url = (
        os.environ.get(args.metadata_url_env) if args.metadata_url_env else None
    )
    if args.metadata_url_env and not args.metadata_url:
        parser.error("Metadata URL environment variable is missing")
    if args.initialize_metadata and not args.metadata_url:
        parser.error("--initialize-metadata requires --metadata-url-env")
    return args
