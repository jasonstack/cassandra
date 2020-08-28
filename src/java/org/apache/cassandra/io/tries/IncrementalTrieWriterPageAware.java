/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Incremental builders of on-disk tries which packs trie stages into disk cache pages.
 *
 * The incremental core is as in {@code IncrementalTrieWriterSimple}, which this augments by:
 *   - calculating branch sizes reflecting the amount of data that needs to be written to store the trie
 *     branch rooted at each node
 *   - delaying writing any part of a completed node until its branch size is above the page size
 *   - laying out (some of) its children branches (each smaller than a page) to be contained within a page
 *   - adjusting the branch size to reflect the fact that the children are now written (i.e. removing their size)
 * The process is bottom-up, i.e. pages are packed at the bottom and the root page is usually smaller. This is not
 * optimally efficient as the root page is normally always in cache and we should be using the most of it. Unfortunately
 * this is unavoidable for an incremental process.
 *
 * As an example, taking a sample page size fitting 4 nodes, a simple trie would be split like this:
 * Node 0 |
 *   -a-> | Node 1
 *        |   -s-> Node 2
 *        |          -k-> Node 3 (payload 1)
 *        |          -s-> Node 4 (payload 2)
 *        -----------------------------------
 *   -b-> Node 5 |
 *          -a-> |Node 6
 *               |  -n-> Node 7
 *               |         -k-> Node 8 (payload 3)
 *               |                -s-> Node 9 (payload 4)
 * where lines denote page boundaries.
 *
 * The process itself will start by adding "ask" which adds three nodes after the root to the stack. Adding "ass"
 * completes Node 3, setting its branch a size of 1 and replaces it on the stack with Node 4.
 * The step of adding "bank" starts by completing Node 4 (size 1), Node 2 (size 3), Node 1 (size 4), then adds 4 more
 * nodes to the stack. Adding "banks" descends one more node.
 * The trie completion step completes nodes 9 (size 1), 8 (size 2), 7 (size 3), 6 (size 4), 5 (size 5). Since the size
 * of node 5 is above the page size, the algorithm lays out its children. Nodes 6, 7, 8, 9 are written in order. The
 * size of node 5 is now just the size of it individually, 1. The process continues with completing Node 0 (size 6).
 * This is bigger than the page size, so some of its children need to be written. The algorithm takes the largest,
 * Node 1, and lays it out with its children in the file. Node 0 now has an adjusted size of 2 which is below the
 * page size and we can continue the process.
 * Since this was the root of the trie, the current page is padded and the remaining nodes 0, 5 are written.
 */
public class IncrementalTrieWriterPageAware<Value>
extends IncrementalTrieWriterBase<Value, DataOutputPlus, IncrementalTrieWriterPageAware.Node<Value>>
implements IncrementalTrieWriter<Value>
{
    final int maxBytesPerPage;

    private final static Comparator<Node<?>> BRANCH_SIZE_COMPARATOR = (l, r) ->
    {
        // Smaller branches first.
        int c = Integer.compare(l.branchSize + l.nodeSize, r.branchSize + r.nodeSize);
        if (c != 0)
            return c;

        // Then order by character, which serves several purposes:
        // - enforces inequality to make sure equal sizes aren't treated as duplicates,
        // - makes sure the item we use for comparison key comes greater than all equal-sized nodes,
        // - orders equal sized items so that most recently processed (and potentially having closer children) comes
        //   last and is thus the first one picked for layout.
        c = Integer.compare(l.transition, r.transition);

        assert c != 0 || l == r;
        return c;
    };

    IncrementalTrieWriterPageAware(TrieSerializer<Value, ? super DataOutputPlus> trieSerializer, DataOutputPlus dest)
    {
        super(trieSerializer, dest, new Node<>((byte) 0));
        this.maxBytesPerPage = dest.maxBytesInPage();
    }

    @Override
    public void reset()
    {
        reset(new Node<>((byte) 0));
    }

    @Override
    Node<Value> performCompletion() throws IOException
    {
        Node<Value> root = super.performCompletion();

        int actualSize = recalcTotalSizeRecursive(root, dest.position());
        int bytesLeft = dest.bytesLeftInPage();
        if (actualSize > bytesLeft)
        {
            if (actualSize <= maxBytesPerPage)
            {
                dest.padToPageBoundary();
                bytesLeft = maxBytesPerPage;
                // position changed, recalculate again
                actualSize = recalcTotalSizeRecursive(root, dest.position());
            }

            if (actualSize > bytesLeft)
            {
                // Still greater. Lay out children separately.
                layoutChildren(root);

                // Pad if needed and place.
                if (root.nodeSize > dest.bytesLeftInPage())
                {
                    dest.padToPageBoundary();
                    // Recalculate again as pointer size may have changed, triggering assertion in writeRecursive.
                    recalcTotalSizeRecursive(root, dest.position());
                }
            }
        }


        root.finalizeWithPosition(writeRecursive(root));
        return root;
    }

    @Override
    void complete(Node<Value> node) throws IOException
    {
        assert node.filePos == -1;

        int branchSize = 0;
        for (Node<Value> child : node.children)
            branchSize += child.branchSize + child.nodeSize;

        node.branchSize = branchSize;

        int nodeSize = serializer.sizeofNode(node, dest.position());
        if (nodeSize + branchSize < maxBytesPerPage)
        {
            // Good. This node and all children will (most probably) fit page.
            node.nodeSize = nodeSize;
            node.hasOutOfPageChildren = false;
            node.hasOutOfPageInBranch = false;

            for (Node<Value> child : node.children)
                if (child.filePos != -1)
                    node.hasOutOfPageChildren = true;
                else if (child.hasOutOfPageChildren || child.hasOutOfPageInBranch)
                    node.hasOutOfPageInBranch = true;

            return;
        }

        // Cannot fit. Lay out children; The current node will be marked with one with out-of-page children.
        layoutChildren(node);
    }

    private void layoutChildren(Node<Value> node) throws IOException
    {
        assert node.filePos == -1;

        NavigableSet<Node<Value>> children = new TreeSet<>(BRANCH_SIZE_COMPARATOR);
        for (Node<Value> child : node.children)
            if (child.filePos == -1)
                children.add(child);

        int bytesLeft = dest.bytesLeftInPage();
        Node<Value> cmp = new Node<>(256); // goes after all equal-sized unplaced nodes (whose transition character is 0-255)
        cmp.nodeSize = 0;
        while (!children.isEmpty())
        {
            cmp.branchSize = bytesLeft;
            Node<Value> child = children.headSet(cmp, true).pollLast();    // grab biggest that could fit
            if (child == null)
            {
                dest.padToPageBoundary();
                bytesLeft = maxBytesPerPage;
                child = children.pollLast();       // just biggest
            }

            if (child.hasOutOfPageChildren || child.hasOutOfPageInBranch)
            {
                // We didn't know what size this branch will actually need to be, node's children may be far.
                // We now know where we would place it, so let's reevaluate size.
                int actualSize = recalcTotalSizeRecursive(child, dest.position());
                if (actualSize > bytesLeft)
                {
                    if (bytesLeft == maxBytesPerPage)
                    {
                        // Branch doesn't even fit in a page.

                        // Note: In this situation we aren't actually making the best choice as the layout should have
                        // taken place at the child (which could have made the current parent small enough to fit).
                        // This is not trivial to fix but should be very rare.

                        layoutChildren(child);
                        bytesLeft = dest.bytesLeftInPage();

                        assert (child.filePos == -1);
                    }

                    // Doesn't fit, but that's probably because we don't have a full page. Put it back with the new
                    // size and retry when we do have enough space.
                    children.add(child);
                    continue;
                }
            }

            child.finalizeWithPosition(writeRecursive(child));
            bytesLeft = dest.bytesLeftInPage();
        }

        // The sizing below will use the branch size, so make sure it's set.
        node.branchSize = 0;
        node.hasOutOfPageChildren = true;
        node.hasOutOfPageInBranch = false;
        node.nodeSize = serializer.sizeofNode(node, dest.position());
    }

    /**
     * Simple framework for executing recursion using on-heap linked trace to avoid stack overruns.
     */
    static abstract class Recursion<NodeType>
    {
        final Recursion<NodeType> parent;
        final NodeType node;
        final Iterator<NodeType> childIterator;

        Recursion(NodeType node, Iterator<NodeType> childIterator, Recursion<NodeType> parent)
        {
            this.parent = parent;
            this.node = node;
            this.childIterator = childIterator;
        }

        /**
         * Make a child Recursion object for the given node and initialize it as necessary to continue processing
         * with it.
         *
         * May return null if the recursion does not need to continue inside the child branch.
         */
        abstract Recursion<NodeType> makeChild(NodeType child);

        /**
         * Complete the processing this Recursion object.
         *
         * Note: this method is not called for the nodes for which makeChild() returns null.
         */
        abstract void complete() throws IOException;

        /**
         * Complete processing of the given child (possibly retrieve data to apply to any accumulation performed
         * in this Recursion object).
         *
         * This is called when processing a child completes, including when recursion inside the child branch
         * is skipped by makeChild() returning null.
         */
        void completeChild(NodeType child)
        {}

        /**
         * Recursive process, in depth-first order, the branch rooted at this recursion node.
         *
         * Returns this.
         */
        Recursion<NodeType> process() throws IOException
        {
            Recursion<NodeType> curr = this;

            while (true)
            {
                if (curr.childIterator.hasNext())
                {
                    NodeType child = curr.childIterator.next();
                    Recursion<NodeType> childRec = curr.makeChild(child);
                    if (childRec != null)
                        curr = childRec;
                    else
                        curr.completeChild(child);
                }
                else
                {
                    curr.complete();
                    Recursion<NodeType> parent = curr.parent;
                    if (parent == null)
                        return curr;
                    parent.completeChild(curr.node);
                    curr = parent;
                }
            }
        }
    }

    class RecalcTotalSizeRecursion extends Recursion<Node<Value>>
    {
        final long nodePosition;
        int sz;

        RecalcTotalSizeRecursion(Node<Value> node, Recursion<Node<Value>> parent, long nodePosition)
        {
            super(node, node.children.iterator(), parent);
            sz = 0;
            this.nodePosition = nodePosition;
        }

        @Override
        Recursion<Node<Value>> makeChild(Node<Value> child)
        {
            if (child.hasOutOfPageInBranch)
                return new RecalcTotalSizeRecursion(child, this, nodePosition + sz);
            else
                return null;
        }

        @Override
        void complete()
        {
            node.branchSize = sz;
        }

        @Override
        void completeChild(Node<Value> child)
        {
            // This will be called for nodes that were recursively processed as well as the ones that weren't.

            // The sizing below will use the branch size calculated above. Since that can change on out-of-page in branch,
            // we need to recalculate the size if either flag is set.
            if (child.hasOutOfPageChildren || child.hasOutOfPageInBranch)
            {
                long childPosition = this.nodePosition + sz;
                child.nodeSize = serializer.sizeofNode(child, childPosition + child.branchSize);
            }

            sz += child.branchSize + child.nodeSize;
        }
    }

    private int recalcTotalSizeRecursive(Node<Value> node, long nodePosition) throws IOException
    {
        return recalcTotalSizeRecursiveOnStack(node, nodePosition, 0);
    }

    private int recalcTotalSizeRecursiveOnStack(Node<Value> node, long nodePosition, int depth) throws IOException
    {
        if (node.hasOutOfPageInBranch)
        {
            int sz = 0;
            for (Node<Value> child : node.children)
            {
                if (depth < 64)
                    sz += recalcTotalSizeRecursiveOnStack(child, nodePosition + sz, depth + 1);
                else
                    sz += recalcTotalSizeRecursiveOnHeap(child, nodePosition + sz);
            }
            node.branchSize = sz;
        }

        // The sizing below will use the branch size calculated above. Since that can change on out-of-page in branch,
        // we need to recalculate the size if either flag is set.
        if (node.hasOutOfPageChildren || node.hasOutOfPageInBranch)
            node.nodeSize = serializer.sizeofNode(node, nodePosition + node.branchSize);

        return node.branchSize + node.nodeSize;
    }

    private int recalcTotalSizeRecursiveOnHeap(Node<Value> node, long nodePosition) throws IOException
    {
        if (node.hasOutOfPageInBranch)
            new RecalcTotalSizeRecursion(node, null, nodePosition).process();

        if (node.hasOutOfPageChildren || node.hasOutOfPageInBranch)
            node.nodeSize = serializer.sizeofNode(node, nodePosition + node.branchSize);

        return node.branchSize + node.nodeSize;
    }

    class WriteRecursion extends Recursion<Node<Value>>
    {
        long nodePosition;

        WriteRecursion(Node<Value> node, Recursion<Node<Value>> parent)
        {
            super(node, node.children.iterator(), parent);
            nodePosition = dest.position();
        }

        @Override
        Recursion<Node<Value>> makeChild(Node<Value> child)
        {
            if (child.filePos == -1)
                return new WriteRecursion(child, this);
            else
                return null;
        }

        @Override
        void complete() throws IOException
        {
            nodePosition = nodePosition + node.branchSize;
            assert dest.position() == nodePosition
                    : "Expected node position to be " + nodePosition + " but got " + dest.position() + " after writing children.\n" + dumpNode(node, dest.position());

            serializer.write(dest, node, nodePosition);

            assert dest.position() == nodePosition + node.nodeSize
                    || dest.paddedPosition() == dest.position() // For PartitionIndexTest.testPointerGrowth where position may jump on page boundaries.
                    : "Expected node position to be " + (nodePosition + node.nodeSize) + " but got " + dest.position() + " after writing node, nodeSize " + node.nodeSize + ".\n" + dumpNode(node, nodePosition);

            node.filePos = nodePosition;
        }
    }

    private long writeRecursive(Node<Value> node) throws IOException
    {
        return writeRecursiveOnStack(node, 0);
    }

    private long writeRecursiveOnStack(Node<Value> node, int depth) throws IOException
    {
        long nodePosition = dest.position();
        for (Node<Value> child : node.children)
            if (child.filePos == -1)
            {
                if (depth < 64)
                    child.filePos = writeRecursiveOnStack(child, depth + 1);
                else
                    child.filePos = writeRecursiveOnHeap(child);
            }

        nodePosition += node.branchSize;
        assert dest.position() == nodePosition
                : "Expected node position to be " + nodePosition + " but got " + dest.position() + " after writing children.\n" + dumpNode(node, dest.position());

        serializer.write(dest, node, nodePosition);

        assert dest.position() == nodePosition + node.nodeSize
                || dest.paddedPosition() == dest.position() // For PartitionIndexTest.testPointerGrowth where position may jump on page boundaries.
                : "Expected node position to be " + (nodePosition + node.nodeSize) + " but got " + dest.position() + " after writing node, nodeSize " + node.nodeSize + ".\n" + dumpNode(node, nodePosition);
        return nodePosition;
    }

    private long writeRecursiveOnHeap(Node<Value> node) throws IOException
    {
        return new WriteRecursion(node, null).process().node.filePos;
    }

    private String dumpNode(Node<Value> node, long nodePosition)
    {
        StringBuilder res = new StringBuilder(String.format("At %,d(%x) type %s child count %s nodeSize %,d branchSize %,d %s%s\n",
                                                            nodePosition, nodePosition,
                                                            TrieNode.typeFor(node, nodePosition), node.childCount(), node.nodeSize, node.branchSize,
                                                            node.hasOutOfPageChildren ? "C" : "",
                                                            node.hasOutOfPageInBranch ? "B" : ""));
        for (Node<Value> child : node.children)
            res.append(String.format("Child %2x at %,d(%x) type %s child count %s size %s nodeSize %,d branchSize %,d %s%s\n",
                                     child.transition & 0xFF,
                                     child.filePos,
                                     child.filePos,
                                     child.children != null ? TrieNode.typeFor(child, child.filePos) : "n/a",
                                     child.children != null ? child.childCount() : "n/a",
                                     child.children != null ? serializer.sizeofNode(child, child.filePos) : "n/a",
                                     child.nodeSize,
                                     child.branchSize,
                                     child.hasOutOfPageChildren ? "C" : "",
                                     child.hasOutOfPageInBranch ? "B" : ""));

        return res.toString();
    }

    @Override
    public PartialTail makePartialRoot() throws IOException
    {
        // The expectation is that the partial tail will be in memory, so we don't bother with page-fitting.
        // We could also send some completed children to disk, but that could make suboptimal layout choices so we'd
        // rather not. Just write anything not written yet to a buffer, from bottom to top, and we're done.
        try (DataOutputBuffer buf = new DataOutputBuffer())
        {
            PTail tail = new PTail();
            // Readers ask rebufferers for page-aligned positions, so make sure tail starts at one.
            // "Padding" of the cutoff point may leave some unaddressable space in the constructed file view.
            // Nothing will point to it, though, so that's fine.
            tail.cutoff = dest.paddedPosition();
            tail.count = count;
            tail.root = writePartialRecursive(stack.getFirst(), buf, tail.cutoff);
            tail.tail = buf.trimmedBuffer();
            return tail;
        }
    }

    class WritePartialRecursion extends Recursion<Node<Value>>
    {
        final DataOutputPlus dest;
        final long baseOffset;
        final long startPosition;
        final List<Node<Value>> childrenToClear;

        WritePartialRecursion(Node<Value> node, WritePartialRecursion parent)
        {
            super(node, node.children.iterator(), parent);
            this.dest = parent.dest;
            this.baseOffset = parent.baseOffset;
            this.startPosition = dest.position() + baseOffset;
            childrenToClear = new ArrayList<>();
        }

        WritePartialRecursion(Node<Value> node, DataOutputPlus dest, long baseOffset)
        {
            super(node, node.children.iterator(), null);
            this.dest = dest;
            this.baseOffset = baseOffset;
            this.startPosition = dest.position() + baseOffset;
            childrenToClear = new ArrayList<>();
        }

        @Override
        Recursion<Node<Value>> makeChild(Node<Value> child)
        {
            if (child.filePos == -1)
            {
                childrenToClear.add(child);
                return new WritePartialRecursion(child, this);
            }
            else
                return null;
        }

        @Override
        void complete() throws IOException
        {
            long nodePosition = dest.position() + baseOffset;

            if (node.hasOutOfPageInBranch)
            {
                // Update the branch size with the size of what we have just written. This may be used by the node's
                // maxPositionDelta and it's a better approximation for later fitting calculations.
                node.branchSize = (int) (nodePosition - startPosition);
            }

            serializer.write(dest, node, nodePosition);

            if (node.hasOutOfPageChildren || node.hasOutOfPageInBranch)
            {
                // Update the node size with what we have just seen. It's a better approximation for later fitting
                // calculations.
                long endPosition = dest.position() + baseOffset;
                node.nodeSize = (int) (endPosition - nodePosition);
            }

            for (Node<Value> child : childrenToClear)
                child.filePos = -1;

            node.filePos = nodePosition;
        }
    }

    private long writePartialRecursive(Node<Value> node, DataOutputPlus dest, long baseOffset) throws IOException
    {
        return writePartialRecursiveOnStack(node, dest, baseOffset, 0);
    }

    private long writePartialRecursiveOnStack(Node<Value> node, DataOutputPlus dest, long baseOffset, int depth) throws IOException
    {
        long startPosition = dest.position() + baseOffset;

        List<Node<Value>> childrenToClear = new ArrayList<>();
        for (Node<Value> child : node.children)
        {
            if (child.filePos == -1)
            {
                childrenToClear.add(child);
                if (depth < 64)
                    child.filePos = writePartialRecursiveOnStack(child, dest, baseOffset, depth + 1);
                else
                    child.filePos = writePartialRecursiveOnHeap(child, dest, baseOffset);
            }
        }

        long nodePosition = dest.position() + baseOffset;

        if (node.hasOutOfPageInBranch)
        {
            // Update the branch size with the size of what we have just written. This may be used by the node's
            // maxPositionDelta and it's a better approximation for later fitting calculations.
            node.branchSize = (int) (nodePosition - startPosition);
        }

        serializer.write(dest, node, nodePosition);

        if (node.hasOutOfPageChildren || node.hasOutOfPageInBranch)
        {
            // Update the node size with what we have just seen. It's a better approximation for later fitting
            // calculations.
            long endPosition = dest.position() + baseOffset;
            node.nodeSize = (int) (endPosition - nodePosition);
        }

        for (Node<Value> child : childrenToClear)
            child.filePos = -1;
        return nodePosition;
    }

    private long writePartialRecursiveOnHeap(Node<Value> node, DataOutputPlus dest, long baseOffset) throws IOException
    {
        new WritePartialRecursion(node, dest, baseOffset).process();
        long pos = node.filePos;
        node.filePos = -1;
        return pos;
    }

    static class Node<Value> extends IncrementalTrieWriterBase.BaseNode<Value, Node<Value>>
    {
        /**
         * Currently calculated size of the branch below this node, not including the node itself.
         * If hasOutOfPageInBranch is true, this may be underestimated as the size
         * depends on the position the branch is written.
         */
        int branchSize = -1;
        /**
         * Currently calculated node size. If hasOutOfPageChildren is true, this may be underestimated as the size
         * depends on the position the node is written.
         */
        int nodeSize = -1;

        /**
         * Whether there is an out-of-page, already written node in the branches below the immediate children of the
         * node.
         */
        boolean hasOutOfPageInBranch = false;
        /**
         * Whether a child of the node is out of page, already written.
         * Forced to true before being set to make sure maxPositionDelta performs its evaluation on non-completed
         * nodes for makePartialRoot.
         */
        boolean hasOutOfPageChildren = true;

        Node(int transition)
        {
            super(transition);
        }

        @Override
        Node<Value> newNode(byte transition)
        {
            return new Node<>(transition & 0xFF);
        }

        public long serializedPositionDelta(int i, long nodePosition)
        {
            assert (children.get(i).filePos != -1);
            return children.get(i).filePos - nodePosition;
        }

        /**
         * The max delta is the delta with either:
         * - the position where the first child not-yet-placed child will be laid out.
         * - the position of the furthest child that is already placed.
         *
         * This method assumes all children's branch and node sizes, as well as this node's branchSize, are already
         * calculated.
         */
        public long maxPositionDelta(long nodePosition)
        {
            // The max delta is the position the first child would be laid out.
            assert (childCount() > 0);

            if (!hasOutOfPageChildren)
                // We need to be able to address the first child. We don't need to cover its branch, though.
                return -(branchSize - children.get(0).branchSize);

            long minPlaced = 0;
            long minUnplaced = 1;
            for (Node<Value> child : children)
            {
                if (child.filePos != -1)
                    minPlaced = Math.min(minPlaced, child.filePos - nodePosition);
                else if (minUnplaced > 0)   // triggers once
                    minUnplaced = -(branchSize - child.branchSize);
            }

            return Math.min(minPlaced, minUnplaced);
        }

        void finalizeWithPosition(long position)
        {
            this.branchSize = 0;                // takes no space in current page
            this.nodeSize = 0;
            this.hasOutOfPageInBranch = false;  // its size no longer needs to be recalculated
            this.hasOutOfPageChildren = false;
            super.finalizeWithPosition(position);
        }

        @Override
        public String toString()
        {
            return String.format("%02x branchSize=%04x nodeSize=%04x %s%s", transition, branchSize, nodeSize, hasOutOfPageInBranch ? "B" : "", hasOutOfPageChildren ? "C" : "");
        }
    }
}
