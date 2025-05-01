from pathlib import Path
from collections import Counter

# 1. Locate the log file next to this script
BASE_DIR = Path(__file__).parent.resolve()
LOG_PATH = BASE_DIR / "log.txt"

def main():
    # 2. Read all the lines
    try:
        with LOG_PATH.open("r") as file:
            logs = file.readlines() # Pulls every line into a list
    except FileNotFoundError:
        print(f"Error: cannot find log file at {LOG_PATH!r}")
        return

    # 3. Initialize counters & collectors
    info_count    = 0
    error_count   = 0
    warning_count = 0
    error_messages = []
    hours          = []

    # 4. Parse each line
    for line in logs:
        parts = line.strip().split()
        if len(parts) < 3:
            continue

        # a) Timestamp → hour
        timestamp = parts[1]
        hour = timestamp.split(":")[0]
        hours.append(hour)

        # b) Severity and message
        level = parts[2]
        if level == "INFO":
            info_count += 1
        elif level == "ERROR":
            error_count += 1
            message = " ".join(parts[3:])
            error_messages.append(message)
        elif level == "WARNING":
            warning_count += 1

    # 5. Print basic summary
    print("Log Summary:")
    print(f"  INFO messages:    {info_count}")
    print(f"  ERROR messages:   {error_count}")
    print(f"  WARNING messages: {warning_count}")

    # 6. Most common error
    if error_messages:
        most_common_error, count = Counter(error_messages).most_common(1)[0]
        print(f"\nMost common error:\n  \"{most_common_error}\" ({count} times)")
    else:
        print("\nNo errors found.")

    # 7. Logs per hour
    print("\nLogs per hour:")
    for hour in sorted(Counter(hours)):
        print(f"{hour}:00 - {Counter(hours)[hour]} logs")

if __name__ == "__main__":
    main()
