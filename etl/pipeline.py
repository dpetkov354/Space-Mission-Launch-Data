import subprocess
import os
import sys


def run_script(script_name):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(script_dir, script_name)

        print(f"Running {script_name} with Python: {sys.executable}")
        print(f"Working directory: {script_dir}")

        result = subprocess.run(
            [sys.executable, script_path],
            cwd=script_dir,
            check=True,
            capture_output=True,
            text=True
        )

        print(f"Output from {script_name}:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error while running {script_name}:\n{e.stderr}")
        raise


if __name__ == "__main__":
    scripts = ["extract.py", "transform.py", "load.py"]

    for script in scripts:
        run_script(script)

    print("Pipeline executed successfully!")
