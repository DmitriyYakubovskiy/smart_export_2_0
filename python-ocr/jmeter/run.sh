#!/bin/sh
set -eu
file="/input/${TEST_FILE:-medical_data_nlp_ru.pdf}"
if [ ! -f "$file" ]; then
    echo "Input not found: $file. Copy a valid synthetic PDF/PNG to python-ocr/jmeter-input."
    exit 2
fi
case "$file" in
    *.[pP][dD][fF]) mime=application/pdf ;;
    *.[pP][nN][gG]) mime=image/png ;;
    *) echo 'Only PDF and PNG are supported'; exit 2 ;;
esac
bytes=$(wc -c < "$file")
if [ "$bytes" -eq 0 ] || [ "$bytes" -gt 5242880 ]; then
    echo 'File must contain 1 byte to 5 MiB'; exit 2
fi
for setting in USERS LOOPS RAMP_UP DURATION PAUSE_MS; do
    eval "value=\${$setting:-}"
    case "$value" in ''|*[!0-9]*) echo "$setting must be an integer"; exit 2;; esac
done
if [ "$USERS" -lt 1 ] || [ "$USERS" -gt 100 ] || [ "$LOOPS" -lt 1 ] || [ "$DURATION" -lt 1 ]; then
    echo 'USERS must be 1..100; LOOPS and DURATION must be positive'; exit 2
fi
# Do not begin a load run if the API cannot be reached.
curl -fsS --connect-timeout 5 --max-time 10 http://api:8000/health > /dev/null
run="/reports/$(date -u +%Y%m%d-%H%M%S)-$$"
mkdir -p "$run"
printf 'users=%s\nloops_per_user=%s\nramp_up_seconds=%s\nduration_limit_seconds=%s\npause_ms=%s\ninput_bytes=%s\n' "$USERS" "$LOOPS" "$RAMP_UP" "$DURATION" "$PAUSE_MS" "$bytes" > "$run/settings.txt"
sha256sum "$file" >> "$run/settings.txt"
echo "Starting up to $((USERS * LOOPS)) uploads. Reports: $run"
jmeter -n -t /tests/upload.jmx -l "$run/results.jtl" -j "$run/jmeter.log" \
    -Jhost=api -Jport=8000 -Jfile="$file" -Jmime="$mime" \
    -Jusers="$USERS" -Jloops="$LOOPS" -Jramp="$RAMP_UP" \
    -Jduration="$DURATION" -Jpause="$PAUSE_MS" \
    -Jsample_variables=document_id -Jjmeter.save.saveservice.output_format=csv \
    -Jjmeter.save.saveservice.assertion_results_failure_message=true \
    -e -o "$run/html"
echo "Finished. Open html/index.html inside $run. Inspect Errors and failed assertions."
echo '202 measures queue acceptance, NOT OCR completion. A zero CLI exit code does not mean all samples passed.'
