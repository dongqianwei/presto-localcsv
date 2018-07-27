package com.facebook.presto.localcsv;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigBinder;

public class LocalCsvModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(LocalCsvConnector.class).in(Scopes.SINGLETON);
        binder.bind(LocalCsvMetadata.class).in(Scopes.SINGLETON);
        binder.bind(LocalCsvSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LocalCsvRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(LocalCsvUtils.class).in(Scopes.SINGLETON);
        binder.bind(LocalCsvHandleResolver.class).in(Scopes.SINGLETON);

        ConfigBinder.configBinder(binder).bindConfig(LocalCsvConfig.class);
    }
}
