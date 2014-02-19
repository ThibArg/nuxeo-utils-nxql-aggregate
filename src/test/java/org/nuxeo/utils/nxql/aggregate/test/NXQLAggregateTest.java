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

package org.nuxeo.utils.nxql.aggregate.test;

import static org.junit.Assert.*;
import nuxeo.utils.nxql.aggregate.NXQLAggregate;
import nuxeo.utils.nxql.aggregate.NXQLAggregateResults;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.platform.test.PlatformFeature;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import com.google.inject.Inject;

/**
 * @author Thibaud Arguillere
 *
 * WARNING: To store some numeric values, we use the common:size field
 * This is bad. But very convenient => no need to create a new schema,
 * deploy, etc.
 */

@RunWith(FeaturesRunner.class)
@Features({PlatformFeature.class})
public class NXQLAggregateTest {

    private static final int kSTART_AT = 1;
    private static final int kHOW_MANY = 10;

    private double _expectedResult_Sum = 0;
    private double _expectedResult_Min = kSTART_AT;
    private double _expectedResult_Max = kHOW_MANY;
    private double _expectedResult_Average = 0;
    private double _expectedResult_Count = kHOW_MANY;
    private NXQLAggregateResults _expectedResult_results = new NXQLAggregateResults();

    @Inject
    CoreSession session;

    @Before
    public void initRepo() throws Exception {
        session.removeChildren(session.getRootDocument().getRef());
        session.save();

        for(int i = kSTART_AT; i <= kHOW_MANY; i++) {
            DocumentModel doc = session.createDocumentModel("/", "doc-" + i, "File");
            doc.setPropertyValue("dc:title", "doc-" + i);
            doc.setPropertyValue("common:size", i);
            doc = session.createDocument(doc);
            session.save();

            _expectedResult_Sum += i;
        }
        _expectedResult_Average = _expectedResult_Sum / _expectedResult_Count;

        _expectedResult_results.setValues(_expectedResult_Sum, _expectedResult_Min, _expectedResult_Max, _expectedResult_Average, _expectedResult_Count);

    }

    @Test
    public void testNXQLAggregateResults() throws Exception {
        NXQLAggregateResults results = new NXQLAggregateResults(0, 1, 2, 3, 4);
        NXQLAggregateResults other = new NXQLAggregateResults(results);

        assertEquals("testing testNXQLAggregateResults.equals()", results, other);

        other.setAverage(123456);
        assertFalse(results.equals(other));

    }

    @Test
    public void testEachAggregateFunction() throws Exception {

        double value = 0.0;
        NXQLAggregate agg = new NXQLAggregate(session,
                                                "common:size",
                                                "File",
                                                "",
                                                true,
                                                true,
                                                true);

        value = agg.sum();
        assertTrue("Testing Sum, value=" + value + ", expected= " + _expectedResult_Sum, _expectedResult_Sum == value);

        value = agg.min();
        assertTrue("Testing Min, value=" + value + ", expected= " + _expectedResult_Min, _expectedResult_Min == value);

        value = agg.max();
        assertTrue("Testing Max, value=" + value + ", expected= " + _expectedResult_Max, _expectedResult_Max == value);

        value = agg.average();
        assertTrue("Testing Average, value=" + value + ", expected= " + _expectedResult_Average, _expectedResult_Average == value);

        value = agg.count();
        assertTrue("Testing Count, value=" + value + ", expected= " + _expectedResult_Count, _expectedResult_Count == value);

    }

    @Test
    public void testAggregateAll() throws Exception {
        NXQLAggregate agg = new NXQLAggregate(session,
                                                "common:size",
                                                "File",
                                                "",
                                                true,
                                                true,
                                                true);

        NXQLAggregateResults r = agg.aggregate(NXQLAggregate.kSUM);
        assertTrue("Testing aggregate, flag kSum, value=" + r.getSum() + ", expected= " + _expectedResult_Sum, _expectedResult_Sum == r.getSum());

        r = agg.aggregate(NXQLAggregate.kMIN);
        assertTrue("Testing aggregate, flag kMIN, value=" + r.getMin() + ", expected= " + _expectedResult_Min, _expectedResult_Min == r.getMin());

        r = agg.aggregate(NXQLAggregate.kMAX);
        assertTrue("Testing aggregate, flag kMAX, value=" + r.getMax() + ", expected= " + _expectedResult_Max, _expectedResult_Max == r.getMax());

        r = agg.aggregate(NXQLAggregate.kAVERAGE);
        assertTrue("Testing aggregate, flag kAVERAGE, value=" + r.getAverage() + ", expected= " + _expectedResult_Average, _expectedResult_Average == r.getAverage());

        r = agg.aggregate(NXQLAggregate.kCOUNT);
        assertTrue("Testing aggregate, flag kCOUNT, value=" + r.getCount() + ", expected= " + _expectedResult_Count, _expectedResult_Count == r.getCount());

        r = agg.aggregate(NXQLAggregate.kALL);

        assertEquals("Testing aggregate, flag kALL, value=" + r.toJSONString() + ", expected=" + _expectedResult_results.toJSONString(), r, _expectedResult_results);

    }

}
