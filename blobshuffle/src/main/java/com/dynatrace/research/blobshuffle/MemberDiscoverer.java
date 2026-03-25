package com.dynatrace.research.blobshuffle;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface for discovering members of a distributed cache cluster.
 */
public interface MemberDiscoverer extends Closeable {

    /**
     * Returns the list of all known cluster member addresses.
     *
     * @return list of member addresses
     */
    List<String> getMembers();

    /**
     * Returns the address of this member.
     *
     * @return this member's address
     */
    String getThisMember();

    @Override
    void close();
}
