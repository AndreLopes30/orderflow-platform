param(
    [string]$OrderBaseUrl = "http://localhost:8080",
    [string]$NotificationBaseUrl = "http://localhost:8081"
)

$ErrorActionPreference = "Stop"

Invoke-RestMethod -Uri "$OrderBaseUrl/actuator/health/readiness" | Out-Null
Invoke-RestMethod -Uri "$NotificationBaseUrl/actuator/health/readiness" | Out-Null

$order = Invoke-RestMethod -Method Post -Uri "$OrderBaseUrl/orders" -ContentType "application/json" -Body (@{
    customerId = "customer-smoke"
    total = 150.50
} | ConvertTo-Json)

$persisted = Invoke-RestMethod -Uri "$OrderBaseUrl/orders/$($order.id)"
if ($persisted.status -ne "CREATED") {
    throw "Expected CREATED status but received $($persisted.status)"
}

$deadline = (Get-Date).AddSeconds(30)
$notification = $null
while ((Get-Date) -lt $deadline -and $null -eq $notification) {
    try {
        $notification = Invoke-RestMethod -Uri "$NotificationBaseUrl/notifications/orders/$($order.id)"
    } catch {
        if ($_.Exception.Response.StatusCode.value__ -ne 404) {
            throw
        }
        Start-Sleep -Seconds 1
    }
}

if ($null -eq $notification -or $notification.status -ne "SENT") {
    throw "Notification was not processed within 30 seconds"
}

[pscustomobject]@{
    OrderId = $order.id
    OrderStatus = $persisted.status
    NotificationStatus = $notification.status
    NotificationChannel = $notification.channel
} | Format-List
