package com.dynatrace.research.s3shuffler;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class StaticMemberDiscoverer implements MemberDiscoverer {

    private final String thisMember;

    private final List<String> allMembers;

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
}
