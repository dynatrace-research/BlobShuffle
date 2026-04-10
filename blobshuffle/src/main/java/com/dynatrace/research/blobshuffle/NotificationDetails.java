package com.dynatrace.research.blobshuffle;

/**
 * Identifies the location of a sub-batch within a stored object in object storage.
 * A notification produced by the Batcher carries one {@link NotificationDetails} per output
 * partition so that each Debatcher instance can fetch only its own byte range.
 *
 * @param objectPath           the object key in object storage
 * @param offsetStartInclusive start offset of the sub-batch in the object (inclusive)
 * @param offsetEndExclusive   end offset of the sub-batch in the object (exclusive)
 */
public record NotificationDetails(String objectPath, int offsetStartInclusive, int offsetEndExclusive) {
}
