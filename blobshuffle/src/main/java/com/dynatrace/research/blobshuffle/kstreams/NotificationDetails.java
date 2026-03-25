package com.dynatrace.research.blobshuffle.kstreams;

public record NotificationDetails(String objectPath, int offsetStartInclusive, int offsetEndExclusive) {
}
