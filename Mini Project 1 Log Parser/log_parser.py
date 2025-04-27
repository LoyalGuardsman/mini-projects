#!/usr/bin/env python3
import re
import os
import sys
import argparse
import logging
import json
import csv
from collections import Counter

path = os.path.join(os.path.dirname(__file__), "log.txt")

# Regex pattern to parse log lines with named groups
LINE_RE = re.compile(
    r'^(?P<date>\d{4}-\d{2}-\d{2})\s+'
    r'(?P<time>\d{2}:\d{2}:\d{2})\s+'
    r'(?P<level>INFO|ERROR|WARNING)\s+'
    r'(?P<msg>.*)$'
)

def parse_line(line):
    """Parse a log line and return (date, hour, level, msg) or None if malformed."""
    m = LINE_RE.match(line.strip())
    if not m:
        return None
    data = m.groupdict()
    # Extract hour from the time field
    hour = data['time'].split(':', 1)[0]
    return data['date'], hour, data['level'], data['msg']


def analyze_logs(lines, top_n):
    """Analyze log lines, counting levels, errors, and hourly activity."""
    info_ct = err_ct = warn_ct = 0
    err_msgs = []
    hours = []

    for lineno, line in enumerate(lines, 1):
        parsed = parse_line(line)
        if not parsed:
            logging.debug(f"Skipping malformed line {lineno}")
            continue

        _, hour, level, msg = parsed
        hours.append(hour)
        if level == 'INFO':
            info_ct += 1
        elif level == 'ERROR':
            err_ct += 1
            err_msgs.append(msg)
        elif level == 'WARNING':
            warn_ct += 1

    # Build summary dictionary
    summary = {
        'info': info_ct,
        'error': err_ct,
        'warning': warn_ct,
        'top_errors': Counter(err_msgs).most_common(top_n),
        'hourly': Counter(hours)
    }
    return summary


def export_summary(summary, path):
    """Export summary: JSON if .json extension, else CSV of hourly counts."""
    if path.lower().endswith('.json'):
        with open(path, 'w') as f:
            json.dump(summary, f, indent=2)
    else:
        with open(path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['hour', 'count'])
            for hr in sorted(summary['hourly']):
                writer.writerow([hr, summary['hourly'][hr]])


def main():
    parser = argparse.ArgumentParser(
        description='Parse a log file and report counts, top errors, and hourly activity.'
    )
    parser.add_argument('-i', '--input', required=True, help='Path to log file')
    parser.add_argument('-t', '--top', type=int, default=1, help='Show top N errors')
    parser.add_argument('-o', '--output', help='Optional export path (CSV or JSON)')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(levelname)s: %(message)s'
    )

    # Verify input file exists
    if not os.path.isfile(args.input):
        logging.error(f"Log file not found: {args.input}")
        sys.exit(1)

    # Read log lines
    with open(args.input) as f:
        lines = f.readlines()

    # Analyze
    summary = analyze_logs(lines, args.top)

    # Print summary
    logging.info("Log Summary:")
    logging.info(f"  INFO:    {summary['info']}")
    logging.info(f"  ERROR:   {summary['error']}")
    logging.info(f"  WARNING: {summary['warning']}")
    for msg, cnt in summary['top_errors']:
        logging.info(f"Top error: {msg!r} ({cnt} times)")

    logging.info("Logs per hour:")
    for hr in sorted(summary['hourly']):
        logging.info(f"  {hr}:00 – {summary['hourly'][hr]} logs")

    # Optional export
    if args.output:
        export_summary(summary, args.output)
        logging.info(f"Summary exported to {args.output}")


if __name__ == '__main__':
    main()
