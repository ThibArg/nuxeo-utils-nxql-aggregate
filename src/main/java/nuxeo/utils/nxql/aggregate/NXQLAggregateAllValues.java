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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.OperationContext;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;

/**
 * @author Thibaud Arguillere (Nuxeo)
 */
@Operation(id=NXQLAggregateAllValues.ID, category=Constants.CAT_FETCH, label="NXQL Aggregate All Values", description="")
public class NXQLAggregateAllValues {

    public static final String ID = "NXQLAggregate.AllValues";
    private static final Log log = LogFactory.getLog(NXQLAggregateAllValues.class);

    @Context
    protected CoreSession session;

    @Context
    protected OperationContext ctx;

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
    public void run() throws ClientException {
        NXQLAggregate agg = new NXQLAggregate(session,
                                            fieldPath,
                                            documentType,
                                            whereClause,
                                            exludeHiddenInNavigation,
                                            exludeVersions,
                                            exludeDeleted);

        long t = System.currentTimeMillis();
        NXQLAggregateResults results = agg.aggregate( NXQLAggregate.kALL );
        long d = System.currentTimeMillis() - t;
        log.warn( String.format("Aggregate all values: %dms", d) );
        ctx.put(varName, results.toJSONString());
    }

}
