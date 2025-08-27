# Install prerequisites
sudo apt-get update
sudo apt-get install -y ca-certificates curl apt-transport-https lsb-release gnupg

# Add Microsoft package signing key + repo
curl -sL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.gpg
sudo install -o root -g root -m 644 microsoft.gpg /etc/apt/trusted.gpg.d/
AZ_REPO=$(lsb_release -cs)
echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $AZ_REPO main" | \
  sudo tee /etc/apt/sources.list.d/azure-cli.list
rm microsoft.gpg

sudo apt-get update
sudo apt-get install -y azure-cli

az --version






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
