import json
import sys

if __name__ == "__main__":
    results = json.load(sys.stdin)

    results["210"]["races"].pop("10")
    results["210"]["races"]["9"] = "Mayor"
    ald_str = "Alderperson"
    ecps_str = "Council Member, Chicago Police Department"
    for key, val in results["241"]["races"].items():
        if val.startswith(ald_str):
            results["241"]["races"][key] = val[len(ald_str) + 1 :] + f" {ald_str}"
        if val.startswith(ecps_str):
            results["241"]["races"][key] = val[len(ecps_str) + 1 :] + f" {ecps_str}"
    json.dump(results, sys.stdout)
