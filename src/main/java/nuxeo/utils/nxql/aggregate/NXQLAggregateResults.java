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

import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;

/**
 * @author Thibaud Arguillere (Nuxeo)
 *
 * Up to 5.9.1. Starting at 5.9.2, SUM/MIN/etc. are supported by NXQL, use
 * the "master" branch of this plugin
 */
public class NXQLAggregateResults {
    private double sum = 0.0;
    private double min = 0.0;
    private double max = 0.0;
    private double average = 0.0;
    private double count = 0.0;

    public NXQLAggregateResults() {

    }

    public NXQLAggregateResults(double inSum, double inMin, double inMax,
            double inAverage, double inCount) {
        sum = inSum;
        min = inMin;
        max = inMax;
        average = inAverage;
        count = inCount;
    }

    public NXQLAggregateResults(NXQLAggregateResults inOther) {
        setValues(inOther.getSum(), inOther.getMin(), inOther.getMax(), inOther.getAverage(), inOther.getCount());
    }

    public void setValues(double inSum, double inMin, double inMax,
            double inAverage, double inCount) {
        sum = inSum;
        min = inMin;
        max = inMax;
        average = inAverage;
        count = inCount;
    }

    @Override
    public String toString() {
        return toJSONString();
    }

    @Override
    public boolean equals(Object inOther) {
        if(inOther == null) {
            return false;
        }

        if(inOther == this) {
            return true;
        }

        if (!(inOther instanceof NXQLAggregateResults)) {
            return false;
        }

        NXQLAggregateResults theOther = (NXQLAggregateResults)inOther;
        return      sum == theOther.getSum()
                &&  min == theOther.getMin()
                &&  max == theOther.getMax()
                &&  average == theOther.getAverage()
                &&  count == theOther.getCount();
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public String toJSONString() {
        return String.format("{\"sum\":%s,\"min\":%s,\"max\":%s,\"average\":%s,\"count\":%s}",
                sum, min, max, average, count);
    }

    public Blob toBlob() {
        return new StringBlob( toJSONString(), "application/json" );
    }
}
