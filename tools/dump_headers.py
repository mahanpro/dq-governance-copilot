import os
from pathlib import Path

# Config
ROOT = Path(__file__).resolve().parents[1]  # project root
EXTENSIONS = {".py"}  # adjust if needed
EXCLUDE_DIRS = {".git", ".venv", "__pycache__", ".idea", ".vscode", "tools"}
NUM_LINES = 1500  # how many lines from each file


def generate_tree(root: Path, prefix: str = ""):
    """
    Python implementation of the Linux 'tree' command.
    EXCLUDE_DIRS appear but are not expanded.
    """
    entries = sorted(root.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    lines = []

    for idx, entry in enumerate(entries):
        connector = "└── " if idx == len(entries) - 1 else "├── "
        lines.append(prefix + connector + entry.name)

        if entry.is_dir():
            if entry.name in EXCLUDE_DIRS:
                # Show directory name but do NOT expand
                continue

            extension = "    " if idx == len(entries) - 1 else "│   "
            lines.extend(generate_tree(entry, prefix + extension))

    return lines


def iter_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        # filter out excluded dirs so walk doesn't descend into them
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for fname in filenames:
            p = Path(dirpath) / fname
            if p.suffix in EXTENSIONS:
                yield p


def main():
    out_path = ROOT / "tree.txt"

    with out_path.open("w", encoding="utf-8") as out:

        # --- 1. Write project tree at the top ---
        out.write("PROJECT TREE\n")
        out.write("============\n")
        tree_lines = generate_tree(ROOT)
        out.write("\n".join(tree_lines))
        out.write("\n\n\n")

        # --- 2. Write file contents ---
        out.write("FILE CONTENTS\n")
        out.write("=============\n\n")

        for path in sorted(iter_files(ROOT)):
            out.write(f"{path}:\n")
            try:
                with path.open("r", encoding="utf-8", errors="ignore") as f:
                    for i, line in enumerate(f):
                        if i >= NUM_LINES:
                            break
                        out.write(line.rstrip("\n") + "\n")
            except Exception as e:
                out.write(f"# [ERROR reading file: {e}]\n")

            out.write("\n")  # blank line between files


if __name__ == "__main__":
    main()
