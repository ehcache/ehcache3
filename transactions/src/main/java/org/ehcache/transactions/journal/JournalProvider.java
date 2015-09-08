package org.ehcache.transactions.journal;

import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.Service;

/**
 * @author Ludovic Orban
 */
public interface JournalProvider extends Service {

  Journal getJournal(LocalPersistenceService.PersistenceSpaceIdentifier persistentSpaceId);

}
