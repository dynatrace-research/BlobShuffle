package com.dynatrace.research.s3shuffler.kstreams;

public record NotificationDestination(String destination, int partition) {
}
