/*
 * (C) Copyright ${year} Nuxeo SA (http://nuxeo.com/) and others.
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

import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;

/**
 * @author Thibaud Arguillere
 */
@Operation(id=NXQLAggregateOneValue.ID, category=Constants.CAT_FETCH, label="NXQL Aggregate One Value", description="")
public class NXQLAggregateOneValue {

    public static final String ID = "NXQLAggregate.OneValue";

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

    @Param(name = "varName", required = true/*, order = 6*/)
    protected String varName;

    @OperationMethod
    public void run() throws Exception {
        double value = 0.0;
        NXQLAggregate agg = new NXQLAggregate(session,
                                            fieldPath,
                                            documentType,
                                            whereClause,
                                            exludeHiddenInNavigation,
                                            exludeVersions,
                                            exludeDeleted);

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

        ctx.put(varName, value);
    }

}
