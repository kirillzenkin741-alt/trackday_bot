from datetime import datetime
from pathlib import Path
import shutil


def main() -> int:
    source = Path("trackday.db")
    if not source.exists():
        print("backup: trackday.db not found")
        return 1

    backup_dir = Path("backups")
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = backup_dir / f"trackday_{stamp}.db"
    shutil.copy2(source, target)

    backups = sorted(backup_dir.glob("trackday_*.db"))
    keep_last = 14
    for old in backups[:-keep_last]:
        old.unlink(missing_ok=True)

    print(f"backup: created {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

