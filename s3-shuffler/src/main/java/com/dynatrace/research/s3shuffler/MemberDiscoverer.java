package com.dynatrace.research.s3shuffler;

import java.util.List;

public interface MemberDiscoverer {
    List<String> getMembers();

    String getThisMember();
}
