#!/bin/bash
# --- SET THESE VARIABLES ---
ACCOUNT="sldbatchprod"
SAS="sv=2024-11-04&ss=...&sig=..."   # paste full SAS token (include leading ? only if provided)

# --- LIST CONTAINERS (sanity check) ---
echo "== Containers in $ACCOUNT =="
az storage container list \
  --account-name "$ACCOUNT" \
  --sas-token "$SAS" \
  --query "[].name" -o tsv
echo

# --- SEARCH FOR 'incident' IN ALL CONTAINERS ---
echo "== Searching for 'incident' in blob names =="
for c in $(az storage container list --account-name "$ACCOUNT" --sas-token "$SAS" --query "[].name" -o tsv); do
  az storage blob list \
    --account-name "$ACCOUNT" \
    --container-name "$c" \
    --sas-token "$SAS" \
    --query "[?contains(name, 'incident')].{container:'$c',name:name}" -o tsv
done

# --- OPTIONAL: SHOW ONLY UNIQUE 'FOLDER' PREFIXES ---
echo
echo "== Matching folder prefixes =="
for c in $(az storage container list --account-name "$ACCOUNT" --sas-token "$SAS" --query "[].name" -o tsv); do
  az storage blob list \
    --account-name "$ACCOUNT" \
    --container-name "$c" \
    --sas-token "$SAS" \
    --query "[?contains(name, 'incident')].name" -o tsv |
  sed 's#/[^/]*$#/#' | sort -u | sed "s#^#${c}/#"
done
