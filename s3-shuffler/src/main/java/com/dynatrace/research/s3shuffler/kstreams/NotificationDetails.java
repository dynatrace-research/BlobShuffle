package com.dynatrace.research.s3shuffler.kstreams;

public record NotificationDetails(String objectPath, int offsetStartInclusive, int offsetEndExclusive) {
}
