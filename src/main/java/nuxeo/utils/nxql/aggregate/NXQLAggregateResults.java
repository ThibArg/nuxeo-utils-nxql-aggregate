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
 * @since 5.9.2
 */
public class NXQLAggregateResults {
    private double sum = 0.0;
    private double min = 0.0;
    private double max = 0.0;
    private double average = 0.0;
    private double count = 0.0;

    public NXQLAggregateResults() {

    }

    public NXQLAggregateResults(double sum, double min, double max,
            double average, double count) {
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.average = average;
        this.count = count;
    }

    public void setValues(double sum, double min, double max,
            double average, double count) {
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.average = average;
        this.count = count;
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
        return new StringBlob( toJSONString(), "text/plain", "UTF-8" );
    }
}
