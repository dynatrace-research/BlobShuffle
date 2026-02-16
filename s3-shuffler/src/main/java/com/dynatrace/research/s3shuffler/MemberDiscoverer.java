package com.dynatrace.research.s3shuffler;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface MemberDiscoverer extends Closeable {
    List<String> getMembers();

    String getThisMember();

    @Override
    void close();
}
