/*
 * Copyright (c) 2026 Dynatrace LLC. All rights reserved.
 *
 * This software and associated documentation files (the "Software") are being
 * made available by Dynatrace LLC for the sole purpose of illustrating the
 * implementation of certain algorithms which are published. Permission is
 * hereby granted, free of charge, to any person obtaining a copy of the
 * Software, to view and use the Software for internal, non-production,
 * non-commercial purposes only. Without limiting the foregoing, the Software
 * may not (i) be used to process live data or train, fine-tune, enrich or
 * improve any machine learning or foundation model or other artificial
 * intelligence model or system or (ii) distributed, sublicensed, modified, used
 * to provide a service, or sold either alone or as part of or in combination
 * with any other software. The Software shall at all times be considered the
 * proprietary property of Dynatrace LLC.
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.dynatrace.research.blobshuffle;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * {@link OwnerSelectionStrategy} that routes objects using a consistent-hashing ring.
 *
 * <p>Members and objects are hashed into the same 64-bit space. Each member is placed on the ring
 * at {@code virtualNodesPerMember} positions (virtual nodes) to smooth out load imbalance. An
 * object is owned by the first member encountered when walking the ring clockwise from the
 * object's hash, wrapping around at the end.
 *
 * <p>Unlike {@link ModuloOwnerSelectionStrategy}, adding or removing a single member only relocates
 * the objects that fall into that member's ring arcs (on the order of {@code 1/memberCount} of all
 * objects); the ownership of all other objects is unchanged.
 *
 * <p>The ring is derived deterministically from the (sorted) member set and cached; it is rebuilt
 * only when the member set changes.
 */
public final class ConsistentHashingOwnerSelectionStrategy implements OwnerSelectionStrategy {

    /**
     * Default number of ring positions (virtual nodes) per member. Higher values improve load
     * balance at the cost of memory and ring-rebuild time.
     */
    public static final int DEFAULT_VIRTUAL_NODES_PER_MEMBER = 200;

    private final Hasher64 hasher;

    private final int virtualNodesPerMember;

    // Cached ring, rebuilt only when the sorted member set changes. Both fields are updated
    // together; since the ring is a pure function of the member set, a concurrent rebuild simply
    // recomputes an identical ring.
    private volatile List<String> ringMembers = List.of();
    private volatile NavigableMap<Long, String> ring = new TreeMap<>();

    /**
     * Creates a strategy using the default hasher and {@link #DEFAULT_VIRTUAL_NODES_PER_MEMBER}.
     */
    public ConsistentHashingOwnerSelectionStrategy() {
        this(Hashing.komihash4_3(0x81c1f2b68968424dL), DEFAULT_VIRTUAL_NODES_PER_MEMBER);
    }

    /**
     * Creates a strategy using the given hasher and number of virtual nodes per member.
     *
     * @param hasher the hasher used for both member and object-path hashing
     * @param virtualNodesPerMember the number of ring positions per member (must be positive)
     */
    public ConsistentHashingOwnerSelectionStrategy(Hasher64 hasher, int virtualNodesPerMember) {
        if (virtualNodesPerMember < 1) {
            throw new IllegalArgumentException("virtualNodesPerMember must be positive, was " + virtualNodesPerMember);
        }
        this.hasher = hasher;
        this.virtualNodesPerMember = virtualNodesPerMember;
    }

    @Override
    public String selectOwner(String objectPath, List<String> members) {
        NavigableMap<Long, String> currentRing = this.ringFor(members);
        long objectHash = this.hasher.hashStream().putString(objectPath).getAsLong();
        Map.Entry<Long, String> owner = currentRing.ceilingEntry(objectHash);
        if (owner == null) {
            // The object hashes past the last ring position, so it wraps around to the first one.
            owner = currentRing.firstEntry();
        }
        return owner.getValue();
    }

    private NavigableMap<Long, String> ringFor(List<String> members) {
        List<String> sortedMembers = members.stream().sorted().toList();
        if (sortedMembers.equals(this.ringMembers)) {
            return this.ring;
        }
        NavigableMap<Long, String> newRing = new TreeMap<>();
        for (String member : sortedMembers) {
            for (int i = 0; i < this.virtualNodesPerMember; i++) {
                long vnodeHash = this.hasher.hashStream().putString(member).putInt(i).getAsLong();
                newRing.put(vnodeHash, member);
            }
        }
        this.ring = newRing;
        this.ringMembers = sortedMembers;
        return newRing;
    }
}
