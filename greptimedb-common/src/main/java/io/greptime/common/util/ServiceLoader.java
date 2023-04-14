/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.greptime.common.util;

import io.greptime.common.SPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceConfigurationError;
import java.util.function.Supplier;

/**
 * A simple service-provider loading facility (SPI).
 *
 * @author jiachun.fjc
 */
@SuppressWarnings("unused")
public final class ServiceLoader<S> implements Iterable<S> {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceLoader.class);

    private static final String PREFIX = "META-INF/services/";

    // the class or interface representing the service being loaded
    private final Class<S> service;

    // the class loader used to locate, load, and instantiate providers
    private final ClassLoader loader;

    // cached providers, in instantiation order
    private final LinkedHashMap<String, S> providers = new LinkedHashMap<>();

    // the current lazy-lookup iterator
    private LazyIterator lookupIterator;

    public static <S> ServiceLoader<S> load(Class<S> service) {
        return ServiceLoader.load(service, Thread.currentThread().getContextClassLoader());
    }

    public static <S> ServiceLoader<S> load(Class<S> service, ClassLoader loader) {
        return new ServiceLoader<>(service, loader);
    }

    public List<S> sort() {
        Iterator<S> it = iterator();
        List<S> sortList = new ArrayList<>();
        while (it.hasNext()) {
            sortList.add(it.next());
        }

        if (sortList.size() <= 1) {
            return sortList;
        }

        sortList.sort((o1, o2) -> {
            SPI o1Spi = o1.getClass().getAnnotation(SPI.class);
            SPI o2Spi = o2.getClass().getAnnotation(SPI.class);

            int o1Priority = o1Spi == null ? 0 : o1Spi.priority();
            int o2Priority = o2Spi == null ? 0 : o2Spi.priority();

            return -(o1Priority - o2Priority);
        });

        return sortList;
    }

    public S first() {
        Class<S> first = firstClass();

        if (first == null) {
            throw fail(this.service, "could not find any implementation for class");
        }

        return getOrCreateProvider(first);
    }

    public S firstOrDefault(Supplier<S> supplier) {
        Class<S> first = firstClass();
        return first != null ? getOrCreateProvider(first) : supplier.get();
    }

    public Class<S> firstClass() {
        Iterator<Class<S>> it = classIterator();
        Class<S> first = null;
        while (it.hasNext()) {
            Class<S> cls = it.next();
            if (first == null) {
                first = cls;
            } else {
                SPI currSpi = first.getAnnotation(SPI.class);
                SPI nextSpi = cls.getAnnotation(SPI.class);

                int currPriority = currSpi == null ? 0 : currSpi.priority();
                int nextPriority = nextSpi == null ? 0 : nextSpi.priority();

                if (nextPriority > currPriority) {
                    first = cls;
                }
            }
        }

        return first;
    }

    public S find(String implName) {
        for (S s : this.providers.values()) {
            SPI spi = s.getClass().getAnnotation(SPI.class);
            if (spi != null && spi.name().equalsIgnoreCase(implName)) {
                return s;
            }
        }
        while (this.lookupIterator.hasNext()) {
            Class<S> cls = this.lookupIterator.next();
            SPI spi = cls.getAnnotation(SPI.class);
            if (spi != null && spi.name().equalsIgnoreCase(implName)) {
                try {
                    return newProvider(cls);
                } catch (Throwable x) {
                    throw fail(this.service, "provider " + cls.getName() + " could not be instantiated", x);
                }
            }
        }
        throw fail(this.service, "provider " + implName + " not found");
    }

    public void reload() {
        this.providers.clear();
        this.lookupIterator = new LazyIterator(this.service, this.loader);
    }

    private ServiceLoader(Class<S> service, ClassLoader loader) {
        this.service = Ensures.ensureNonNull(service, "service interface cannot be null");
        this.loader = (loader == null) ? ClassLoader.getSystemClassLoader() : loader;
        reload();
    }

    private static ServiceConfigurationError fail(Class<?> service, String msg, Throwable cause) {
        return new ServiceConfigurationError(service.getName() + ": " + msg, cause);
    }

    private static ServiceConfigurationError fail(Class<?> service, String msg) {
        return new ServiceConfigurationError(service.getName() + ": " + msg);
    }

    private static ServiceConfigurationError fail(Class<?> service, URL url, int line, String msg) {
        return fail(service, url + ":" + line + ": " + msg);
    }

    // parse a single line from the given configuration file, adding the name
    // on the line to the names list.
    private int parseLine(Class<?> service, URL u, BufferedReader r, int lc, List<String> names) throws IOException,
            ServiceConfigurationError {

        String ln = r.readLine();
        if (ln == null) {
            return -1;
        }
        int ci = ln.indexOf('#');
        if (ci >= 0) {
            ln = ln.substring(0, ci);
        }
        ln = ln.trim();
        int n = ln.length();
        if (n != 0) {
            if ((ln.indexOf(' ') >= 0) || (ln.indexOf('\t') >= 0)) {
                throw fail(service, u, lc, "illegal configuration-file syntax");
            }
            int cp = ln.codePointAt(0);
            if (!Character.isJavaIdentifierStart(cp)) {
                throw fail(service, u, lc, "illegal provider-class name: " + ln);
            }
            for (int i = Character.charCount(cp); i < n; i += Character.charCount(cp)) {
                cp = ln.codePointAt(i);
                if (!Character.isJavaIdentifierPart(cp) && (cp != '.')) {
                    throw fail(service, u, lc, "Illegal provider-class name: " + ln);
                }
            }
            if (!this.providers.containsKey(ln) && !names.contains(ln)) {
                names.add(ln);
            }
        }
        return lc + 1;
    }

    private Iterator<String> parse(Class<?> service, URL url) {
        ArrayList<String> names = new ArrayList<>();
        try (InputStream in = url.openStream();
                BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            int lc = 1;
            // noinspection StatementWithEmptyBody
            while ((lc = parseLine(service, url, r, lc, names)) >= 0);
        } catch (IOException x) {
            throw fail(service, "error reading configuration file", x);
        }
        return names.iterator();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<S> iterator() {
        return new Iterator<S>() {

            final Iterator<Map.Entry<String, S>> knownProviders = ServiceLoader.this.providers.entrySet().iterator();

            @Override
            public boolean hasNext() {
                return this.knownProviders.hasNext() || ServiceLoader.this.lookupIterator.hasNext();
            }

            @Override
            public S next() {
                if (this.knownProviders.hasNext()) {
                    return this.knownProviders.next().getValue();
                }
                Class<S> cls = ServiceLoader.this.lookupIterator.next();
                return newProvider(cls);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Iterator<Class<S>> classIterator() {
        return new Iterator<Class<S>>() {

            final Iterator<Map.Entry<String, S>> knownProviders = ServiceLoader.this.providers.entrySet().iterator();

            @Override
            public boolean hasNext() {
                return this.knownProviders.hasNext() || ServiceLoader.this.lookupIterator.hasNext();
            }

            @SuppressWarnings("unchecked")
            @Override
            public Class<S> next() {
                if (this.knownProviders.hasNext()) {
                    return (Class<S>) this.knownProviders.next().getValue().getClass();
                }
                return ServiceLoader.this.lookupIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private S getOrCreateProvider(Class<S> cls) {
        S ins = this.providers.get(cls.getName());
        if (ins != null) {
            return ins;
        }

        return newProvider(cls);
    }

    private S newProvider(Class<S> cls) {
        LOG.info("SPI service [{} - {}] loading.", this.service.getName(), cls.getName());
        try {
            S provider = this.service.cast(cls.newInstance());
            this.providers.put(cls.getName(), provider);
            return provider;
        } catch (Throwable x) {
            throw fail(this.service, "provider " + cls.getName() + " could not be instantiated", x);
        }
    }

    private class LazyIterator implements Iterator<Class<S>> {
        Class<S> service;
        ClassLoader loader;
        Enumeration<URL> configs = null;
        Iterator<String> pending = null;
        String nextName = null;

        private LazyIterator(Class<S> service, ClassLoader loader) {
            this.service = service;
            this.loader = loader;
        }

        @Override
        public boolean hasNext() {
            if (this.nextName != null) {
                return true;
            }
            if (this.configs == null) {
                try {
                    String fullName = PREFIX + this.service.getName();
                    if (this.loader == null) {
                        this.configs = ClassLoader.getSystemResources(fullName);
                    } else {
                        this.configs = this.loader.getResources(fullName);
                    }
                } catch (IOException x) {
                    throw fail(this.service, "error locating configuration files", x);
                }
            }
            while ((this.pending == null) || !this.pending.hasNext()) {
                if (!this.configs.hasMoreElements()) {
                    return false;
                }
                this.pending = parse(this.service, this.configs.nextElement());
            }
            this.nextName = this.pending.next();
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Class<S> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            String name = this.nextName;
            this.nextName = null;
            Class<?> cls;
            try {
                cls = Class.forName(name, false, this.loader);
            } catch (ClassNotFoundException x) {
                throw fail(this.service, "provider " + name + " not found");
            }
            if (!this.service.isAssignableFrom(cls)) {
                throw fail(this.service, "provider " + name + " not a subtype");
            }
            return (Class<S>) cls;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns a string describing this service.
     */
    @Override
    public String toString() {
        return "io.greptime.common.util.ServiceLoader[" + this.service.getName() + "]";
    }
}
