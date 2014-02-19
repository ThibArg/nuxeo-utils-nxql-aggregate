/*
 * (C) Copyright 2014 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Thibaud Arguillere (Nuxeo)
 */
package nuxeo.utils.nxql.aggregate;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.IterableQueryResult;

/**
 * @author Thibaud Arguillere (Nuxeo)
 *
 * Up to 5.9.1. Starting at 5.9.2, SUM/MIN/etc. are supported by NXQL, use
 * the "master" branch of this plugin
 */
public class NXQLAggregate {

    public static final int kSUM = 1 << 1;
    public static final int kMIN = 1 << 2;
    public static final int kMAX = 1 << 3;
    public static final int kAVERAGE = 1 << 4;
    public static final int kCOUNT = 1 << 5;
    public static final int kALL = kSUM | kMIN | kMAX | kAVERAGE | kCOUNT;

    protected CoreSession _session;
    private String _statement = "";
    private String _fieldPath = "";

    public NXQLAggregate(CoreSession inSession,
                      String inFieldPath,
                      String inDocumentType,
                      String inWhereClause,
                      boolean inExludeHiddenInNavigation,
                      boolean inExludeVersions,
                      boolean inExludeDeleted) {

        _session = inSession;
        _fieldPath = inFieldPath;
        _statement= "SELECT " + inFieldPath + " FROM " + inDocumentType;

        inWhereClause = inWhereClause == null ? "" : inWhereClause;

        if(inExludeHiddenInNavigation || inExludeVersions || inExludeDeleted) {
            if(inWhereClause.isEmpty()) {
                inWhereClause = "WHERE";
            }
            if(inExludeHiddenInNavigation) {
                inWhereClause += " AND ecm:mixinType != 'HiddenInNavigation'";
            }
            if(inExludeVersions) {
                inWhereClause += " AND ecm:isCheckedInVersion = 0";
            }
            if(inExludeDeleted) {
                inWhereClause += " AND ecm:currentLifeCycleState != 'deleted'";
            }
            inWhereClause = inWhereClause.replace("WHERE AND", "WHERE");
        }
        if(!inWhereClause.isEmpty()) {
            _statement += " " + inWhereClause;
        }
    }


    public NXQLAggregateResults aggregate(int inFlags) throws ClientException {
        IterableQueryResult iqr = null;
        double sum = 0.0, min = Double.MAX_VALUE, max = (Double.MAX_VALUE - 1)
                * -1, average = 0.0, value = 0.0;
        long count = 0;
        boolean doSum = (inFlags & kSUM) != 0,
                doMin = (inFlags & kMIN) != 0,
                doMax = (inFlags & kMAX) != 0,
                doAverage = (inFlags & kAVERAGE) != 0,
                doCount = (inFlags & kCOUNT) != 0;

        try {
            iqr = _session.queryAndFetch(_statement, "NXQL", (Object[]) null);
            Iterator<Map<String, Serializable>> it = iqr.iterator();
            while (it.hasNext()) {
                value = ((Long) it.next().get(_fieldPath)).doubleValue();
                if (doSum || doAverage) {
                    sum += value;
                }
                if (doMin && value < min) {
                    min = value;
                }
                if (doMax && value > max) {
                    max = value;
                }
                if (doCount || doAverage) {
                    count++;
                }
            }
            if (doAverage) {
                average = sum / count;
            }

            return new NXQLAggregateResults(sum, min, max, average, count);

        } finally {
            if (iqr != null) {
                iqr.close();
            }
        }
    }



    /*  ***********************************************************
     *  Just doing one operation: sum, or min, or max, ...
     *  ***********************************************************
     *  To centralize the usage of the iterator and trying to avoid
     *  calling IterableQueryResult.close(), we use:
     *          -> _NXQLAggregateCallback inner class, which is in
     *             in charge of the iteration depending on the kind
     *             of calculation
     *
     *          -> _aggregateOne(), which is the main dispatcher,
     *             handles the  IterableQueryResult and use the
     *             _NXQLAggregateCallback inner class.
     */
    private class _NXQLAggregateCallback {
        private Iterator<Map<String, Serializable>> _it = null;

        public _NXQLAggregateCallback( Iterator<Map<String, Serializable>> inIterator) {
            _it = inIterator;
        }

        public double sum() {
            double sum = 0.0;
            while( _it.hasNext()) {
                sum += ((Long) _it.next().get(_fieldPath)).doubleValue();
            }
            return sum;
        }

        public double min() {
            double min = Double.MAX_VALUE, value = 0.0;
            while( _it.hasNext()) {
                value = ((Long) _it.next().get(_fieldPath)).doubleValue();
                if(value < min) {
                    min = value;
                }
            }
            return min;
        }

        public double max() {
            double max = (Double.MAX_VALUE - 1) * -1, value;
            while( _it.hasNext()) {
                value = ((Long) _it.next().get(_fieldPath)).doubleValue();
                if(value > max) {
                    max = value;
                }
            }
            return max;
        }

        public double average() {
            double sum = 0.0;
            long count = 0;
            while( _it.hasNext()) {
                sum += ((Long) _it.next().get(_fieldPath)).doubleValue();
                count += 1;
            }
            return count > 0 ? sum / count : 0;
        }

        public double count() {
            long count = 0;
            while( _it.hasNext()) {
                _it.next();
                count += 1;
            }
            return count;
        }
    }

    private double _aggregateOne(int inWhat) throws Exception {
        IterableQueryResult iqr = null;
        double value = 0.0;

        try {
            iqr = _session.queryAndFetch(_statement, "NXQL", (Object[]) null);
            Iterator<Map<String, Serializable>> it = iqr.iterator();

            _NXQLAggregateCallback cb = new _NXQLAggregateCallback(it);

            switch (inWhat) {
            case NXQLAggregate.kSUM:
                value = cb.sum();
                break;

            case NXQLAggregate.kMIN:
                value = cb.min();
                break;

            case NXQLAggregate.kMAX:
                value = cb.max();
                break;

            case NXQLAggregate.kAVERAGE:
                value = cb.average();
                break;

            case NXQLAggregate.kCOUNT:
                value = cb.count();
                break;

            default:
                throw new Exception("Invalid aggregate selector");
            }

            return value;

        } finally {
            if(iqr != null) {
                iqr.close();
            }
        }
    }

    public double sum() throws Exception {
        return _aggregateOne(kSUM);
    }

    public double min() throws Exception {
        return _aggregateOne(kMIN);
    }

    public double max() throws Exception {
        return _aggregateOne(kMAX);
    }

    public double average() throws Exception {
        return _aggregateOne(kAVERAGE);
    }

    public double count() throws Exception {
        return _aggregateOne(kCOUNT);
    }
}
