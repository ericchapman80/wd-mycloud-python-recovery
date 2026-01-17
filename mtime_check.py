import argparse
import os
import sqlite3


def build_path(cur, name, parent_id):
    """Reconstruct relative path from Files table hierarchy."""
    parts = [name]
    pid = parent_id
    while pid:
        cur.execute("SELECT name, parentID FROM Files WHERE id=?", (pid,))
        row = cur.fetchone()
        if not row:
            break
        pname, pid = row
        parts.append(pname)
    return os.path.join(*reversed(parts))


def main():
    parser = argparse.ArgumentParser(
        description="Compare DB timestamps to destination mtimes for sample files."
    )
    parser.add_argument("--db", required=True, help="Path to index.db (SQLite)")
    parser.add_argument("--dest", required=True, help="Destination root where files were copied")
    parser.add_argument("--limit", type=int, default=5, help="Number of sample files to check")
    args = parser.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, name, parentID,
               COALESCE(imageDate, videoDate, cTime, birthTime)/1000.0 AS ts
        FROM Files
        WHERE mimeType != 'application/x.wd.dir'
        LIMIT ?
        """,
        (args.limit,),
    )
    rows = cur.fetchall()

    if not rows:
        print("No rows returned from Files table.")
        return

    for fid, name, parent_id, ts in rows:
        rel = build_path(cur, name, parent_id)
        dest_path = os.path.join(args.dest, rel)
        if not os.path.exists(dest_path):
            print(f"Missing in dest: {dest_path}")
            continue
        mtime = os.path.getmtime(dest_path)
        diff = mtime - ts
        print(f"{dest_path}\n  db_ts={ts}  mtime={mtime}  diff={diff:.1f}s")


if __name__ == "__main__":
    main()
