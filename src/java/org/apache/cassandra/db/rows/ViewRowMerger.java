package org.apache.cassandra.db.rows;

import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Row.Deletion;
import org.apache.cassandra.db.rows.Row.Merger;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

public class ViewRowMerger extends Merger
{
    private final Row[] rows;
    private final List<Iterator<ColumnData>> columnDataIterators;

    private final int nowInSec;
    private Clustering clustering;
    private int rowsToMerge;
    private int lastRowSet = -1;

    private final List<ColumnData> dataBuffer = new ArrayList<>();
    private final ColumnDataReducer columnDataReducer;

    public ViewRowMerger(int size, int nowInSec, boolean hasComplex)
    {
        super(size, nowInSec, hasComplex);
        this.rows = new Row[size];
        this.columnDataIterators = new ArrayList<>(size);
        this.columnDataReducer = new ColumnDataReducer(size, nowInSec, hasComplex);
        this.nowInSec = nowInSec;
    }

    public void clear()
    {
        dataBuffer.clear();
        Arrays.fill(rows, null);
        columnDataIterators.clear();
        rowsToMerge = 0;
        lastRowSet = -1;
    }

    public void add(int i, Row row)
    {
        clustering = row.clustering();
        rows[i] = row;
        ++rowsToMerge;
        lastRowSet = i;
    }

    @Override
    public Row merge(Deletion activeDeletion)
    {
        // If for this clustering we have only one row version and have no activeDeletion (i.e. nothing to filter out),
        // then we can just return that single row
        if (rowsToMerge == 1 && activeDeletion.isLive())
        {
            Row row = rows[lastRowSet];
            assert row != null;
            if (row.primaryKeyLivenessInfo().shouldWipeRow(nowInSec))
            {
                return null;
            }
            return row;
        }

        LivenessInfo rowInfo = LivenessInfo.EMPTY;
        Deletion rowDeletion = Deletion.LIVE;
        for (Row row : rows)
        {
            if (row == null)
                continue;

            rowInfo = row.primaryKeyLivenessInfo().merge(rowInfo);
            rowDeletion = row.deletion().merge(rowDeletion);
        }
        // FIXME we don't need both Deletion & DeletionTime
        rowInfo = rowInfo.merge(rowDeletion);
        if (rowInfo.isLive(nowInSec) && rowDeletion.isForView())
        {
            rowDeletion = Deletion.view(DeletionTime.LIVE, Collections.EMPTY_MAP, Collections.EMPTY_MAP); // FIXME check
                                                                                                          // invalidate
                                                                                                          // view-deletion
        }
        // set pkLivenessInfo as empty if entire row should be purged
        if (rowInfo.shouldWipeRow(nowInSec) || !rowInfo.isLive(nowInSec))
            rowInfo = LivenessInfo.EMPTY;

        rowDeletion = rowDeletion.merge(activeDeletion);

        for (Row row : rows)
        {
            // row level comparison
            if (row != null && rowDeletion.deletes(row.primaryKeyLivenessInfo()))
            {
                continue;
            }
            if (row != null && !rowInfo.equals(row.primaryKeyLivenessInfo())
                    && rowInfo.supersedes(row.primaryKeyLivenessInfo()))
            {
                continue;
            }
            if (row != null && row.primaryKeyLivenessInfo().shouldWipeRow(nowInSec))
            {
                // don't add cells if entire row should be purged
                continue;
            }
            columnDataIterators.add(row == null ? Collections.emptyIterator() : row.iterator());
        }

        columnDataReducer.setActiveDeletion(rowDeletion);
        Iterator<ColumnData> merged = MergeIterator.get(columnDataIterators, ColumnData.comparator, columnDataReducer);
        while (merged.hasNext())
        {
            ColumnData data = merged.next();
            if (data != null)
                dataBuffer.add(data);
        }

        // Because some data might have been shadowed by the 'activeDeletion', we could have an empty row
        return rowInfo.isEmpty() && rowDeletion.isLive() && dataBuffer.isEmpty()
                ? null
                : BTreeRow.create(clustering,
                                  rowInfo,
                                  rowDeletion,
                                  BTree.build(dataBuffer, UpdateFunction.<ColumnData> noOp()));
    }

    public Clustering mergedClustering()
    {
        return clustering;
    }

    public Row[] mergedRows()
    {
        return rows;
    }
}