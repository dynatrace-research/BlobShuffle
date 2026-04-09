package com.dynatrace.research.blobshuffle;

public record NotificationDetails(String objectPath, int offsetStartInclusive, int offsetEndExclusive) {
}
