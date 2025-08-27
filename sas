$SasToken = "sv......."
$AccountName = "sldbatchprod"



$ctx = New-AzStorageContext -StorageAccountName $AccountName -SasToken $SasToken
$containers = Get-AzStorageContainer -Context $ctx
foreach ($c in $containers) {
    Get-AzStorageBlob -Container $c.Name -Context $ctx |
        Where-Object { $_.Name -match "incident" } |
        Select-Object @{n='Container';e={$c.Name}}, Name
}
