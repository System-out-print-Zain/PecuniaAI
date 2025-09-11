import subprocess

scripts = [
    "web_scraper.py",
    "document_processor.py",
    "upload_to_cloud.py"
]

def run_script(script):
    print(f"Running {script}...")
    result = subprocess.run(
        ["python3", script],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Error in {script}!\nstderr:\n{result.stderr}")
        raise RuntimeError(f"Script {script} failed")
    else:
        print(f"Finished {script}\nstdout:\n{result.stdout}")

def run_pipeline():
    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    run_pipeline()