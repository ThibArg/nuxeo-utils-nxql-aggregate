/*
 * (C) Copyright ${year} Nuxeo SA (http://nuxeo.com/) and contributors.
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
 *     thibaud
 */

package nuxeo.utils.nxql.aggregate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.impl.blob.StringBlob;

/**
 *
 */
@Operation(id=NXQLAggregateOneFunctionOp.ID, category=Constants.CAT_FETCH, label="NXQL Aggregate: One Function", description="")
public class NXQLAggregateOneFunctionOp {

    public static final String ID = "NXQLAggregate.OneFunction";
    private static final Log log = LogFactory.getLog(NXQLAggregateOneFunctionOp.class);

    @Context
    protected CoreSession session;

    @Context
    protected OperationContext ctx;

    @Param(name = "kind", required = true, widget = Constants.W_OPTION, values = {"Sum", "Min", "Max", "Average", "Count"})
    protected String kind = "Sum";

    @Param(name = "fieldPath", required = true/*, order = 0*/)
    protected String fieldPath;

    @Param(name = "documentType", required = true/*, order = 1*/)
    protected String documentType;

    @Param(name = "whereClause", required = false/*, order = 2*/)
    protected String whereClause;

    @Param(name = "exludeHiddenInNavigation", required = false, values = {"true"}/*, order = 3*/)
    protected boolean exludeHiddenInNavigation = true;

    @Param(name = "exludeVersions", required = false, values = {"true"}/*, order = 4*/)
    protected boolean exludeVersions = true;

    @Param(name = "exludeDeleted", required = false, values = {"true"}/*, order = 5*/)
    protected boolean exludeDeleted = true;

    @Param(name = "varName", required = false/*, order = 6*/)
    protected String varName;

    @OperationMethod
    public Blob run() throws Exception {
        double value = 0.0;
        NXQLAggregate agg = new NXQLAggregate(session,
                                            fieldPath,
                                            documentType,
                                            whereClause,
                                            exludeHiddenInNavigation,
                                            exludeVersions,
                                            exludeDeleted);

        long t = System.currentTimeMillis();
        switch(kind.toLowerCase()) {
        case "sum":
            value = agg.sum();
            break;

        case "min":
            value = agg.min();
            break;

        case "max":
            value = agg.max();
            break;

        case "average":
            value = agg.average();
            break;

        case "count":
            value = agg.count();
            break;

        default:
            throw new ClientException("NXQLAggregateOneValue: Invalid kind (" + kind + ")");
            //break;
        }
        long d = System.currentTimeMillis() - t;
        log.warn( String.format("Aggregate one value (" + kind + ": %dms", d) );

        if(ctx != null && varName != null && !varName.isEmpty()) {
            ctx.put(varName, value);
        }

        return new StringBlob("{\"" + kind.toLowerCase() + "\":" + value + "}", "application/json");
    }
}
