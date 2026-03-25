package com.dynatrace.research.blobshuffle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A {@link MemberDiscoverer} backed by a fixed, statically configured list of cluster members.
 */
public class StaticMemberDiscoverer implements MemberDiscoverer {

    private final String thisMember;

    private final List<String> allMembers;

    /**
     * Creates a new static member discoverer.
     *
     * @param thisMember the address of this member
     * @param otherMembers the addresses of the other cluster members
     */
    public StaticMemberDiscoverer(String thisMember, String... otherMembers) {
        this.thisMember = thisMember;
        this.allMembers = Stream.concat(Stream.of(thisMember), Stream.of(otherMembers)).sorted().toList();
    }

    @Override
    public List<String> getMembers() {
        return this.allMembers;
    }

    @Override
    public String getThisMember() {
        return this.thisMember;
    }

    @Override
    public void close() {
    }
}
