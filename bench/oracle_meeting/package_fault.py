import argparse
import asyncio
import json
import subprocess
from pathlib import Path

from lang2sql.adapters.db.factory import explorer_from_env

contract = {
    "owner": "L2S_VIEW_OWNER",
    "view": "V_PACKAGE_NET",
    "amount": "NET_AMOUNT",
    "period": "BOOKED_ON",
    "aggregation": "SUM",
    "grain": "one order",
    "filters": {"STATUS": "POSTED"},
}


def alter(expression):
    sql = f"""WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER=FREEPDB1;
CREATE OR REPLACE PACKAGE BODY L2S_VIEW_OWNER.MEETING_RULES AS
 FUNCTION net_amount(g NUMBER,d NUMBER,r NUMBER) RETURN NUMBER IS
 BEGIN RETURN {expression}; END;
END;
/
EXIT
"""
    r = subprocess.run(
        ["docker", "exec", "-i", args.container, "sqlplus", "-s", "/ as sysdba"],
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    assert r.returncode == 0, r.stdout


async def main():
    e = explorer_from_env()
    stamp = await e.oracle_metric(contract)
    try:
        alter("g+1")
        try:
            await e.oracle_metric(
                contract, stamp=stamp, sql="SELECT 1 FROM dual", params={}
            )
        except ValueError as ex:
            assert "reconfirm" in str(ex)
            args.output.write_text(
                json.dumps({"changed_package_body_blocked": True, "error": str(ex)})
            )
        else:
            raise AssertionError("Changed dependency was accepted")
    finally:
        alter("g-NVL(d,0)-NVL(r,0)")
    print("Changed PL/SQL package body blocked; fixture restored")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mutate and restore only the dedicated MEETING_RULES fixture package; never use a production container"
    )
    parser.add_argument("--container", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use new output path")
    asyncio.run(main())
