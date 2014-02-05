/* IMPORTANT: This is just a utility class/operation because I was too lazy
 * to create a separate plug-in, etc. So, even if it's in the git repo as of
 * today (say, backup purpose), it must be deleted before releasing the first
 * version.
 * Its purpose it to build Account documents, with values for their the_value_int
 * and the_value_double fields.
 * This is quick and dirty work. It does the job and it's good enough.
 * If I forget to remove it, well. Too bad, but nothing bad will happen actually :->
 */
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

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.runtime.transaction.TransactionHelper;

/**
 * @author Thibaud Arguillere (Nuxeo)
 */
@Operation(id=AAAAATEMP_CREATEDATA.ID, category=Constants.CAT_DOCUMENT, label="AAAAATEMP_CREATEDATA", description="")
public class AAAAATEMP_CREATEDATA {

    public static final String ID = "AAAAATEMP_CREATEDATA";

    private static final Log log = LogFactory.getLog(AAAAATEMP_CREATEDATA.class);

    @Context
    protected CoreSession session;

    @Param(name = "howMany", required = false, values = {"10000"})
    protected long howMany = 10000;

    @Param(name = "commitEvery", required = false, values = {"100"})
    protected long commitEvery = 100;

    @Param(name = "sleepEvery", required = false, values = {"500"})
    protected long sleepEvery = 500;

    @Param(name = "sleepDurationMS", required = false, values = {"200"})
    protected long sleepDurationMS = 200;

    @Param(name = "stopFilePath", required = false)
    protected String stopFilePath = "";

    private int _RandomInt(int inMin, int inMax) {
        // No error check here
        return inMin + (int)(Math.random() * ((inMax - inMin) + 1));
    }

    @OperationMethod
    public DocumentModel run(DocumentModel input) throws ClientException, InterruptedException {
        String mainParentPath = input.getPathAsString();
        int i, count = 0;
        File stopFile = (stopFilePath == null || stopFilePath.isEmpty()) ? null : new File(stopFilePath);

        log.warn("Creating " + howMany + " Account");

        if(stopFile != null && stopFile.exists()) {
            log.warn("The stopFile already exists. Only some Accoutn will be created.");
            log.warn("(you probably forgot to remove/rename if after a previous test)");
        }

        if(howMany <= 0) {
            howMany = 1000;
        }
        if(commitEvery <= 0) {
            commitEvery = 100;
        }
        if(sleepEvery <= 0) {
            sleepEvery = 500;
        }
        if(sleepDurationMS <= 0) {
            sleepDurationMS = 200;
        }

        TransactionHelper.commitOrRollbackTransaction();
        TransactionHelper.startTransaction();
        for(i = 1; i <= howMany; i++) {
            DocumentModel account = session.createDocumentModel(mainParentPath, "a-" + i, "Account");

            account.setPropertyValue("dc:title", "a-" + i);
            account.setPropertyValue("ac:the_value_int", _RandomInt(0, 1000));
            account.setPropertyValue("ac:the_value_double", _RandomInt(0, 1000));

            account = session.createDocument(account);
            session.saveDocument(account);
            if((count++ % 25) == 0) {
                session.save();
            }

            if((count % (int)commitEvery) == 0) {
                log.warn("Committing 100. Total: " + count);
                TransactionHelper.commitOrRollbackTransaction();
                TransactionHelper.startTransaction();

                if(stopFile != null && stopFile.exists()) {
                    break;
                }
            }

            if((count % (int)sleepEvery) == 0) {
                Thread.sleep(sleepDurationMS);
            }

            if((count % 5000) == 0) {
                Thread.sleep(2000);
            }
        }
        TransactionHelper.commitOrRollbackTransaction();
        TransactionHelper.startTransaction();

        log.warn("CREATION OF Account DONE, " + count + " created.");

        return input;
    }

}
