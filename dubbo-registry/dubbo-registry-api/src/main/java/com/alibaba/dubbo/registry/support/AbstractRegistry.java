/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.registry.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractRegistry. (SPI, Prototype, ThreadSafe)
 *
 */
public abstract class AbstractRegistry implements Registry {

    // URL address separator, used in file cache, service provider URL separation
    /**
     * URL的地址分隔符，在缓存文件中使用，服务提供者的URL分隔
     */
    private static final char URL_SEPARATOR = ' ';
    // URL address separated regular expression for parsing the service provider URL list in the file cache
    /**
     * URL地址分隔正则表达式，用于解析文件缓存中服务提供者URL列表
     * “\s+”则表示匹配任意多个空格， 包括换页、回车、换行、制表符等
     */
    private static final String URL_SPLIT = "\\s+";
    // Log output
    /**
     * 日志输出
     */
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    // Local disk cache, where the special key value.registies records the list of registry centers, and the others are the list of notified service providers
    /**
     * 本地磁盘缓存，有一个特殊的key值为registies，记录的是注册中心列表，其他记录的都是服务提供者列表
     */
    private final Properties properties = new Properties();
    // File cache timing writing
    /**
     * 缓存写入执行器
     */
    private final ExecutorService registryCacheExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("DubboSaveRegistryCache", true));
    // Is it synchronized to save the file
    /**
     *是否同步保存文件标志
     */
    private final boolean syncSaveFile;
    /**
     * 数据版本号
     */
    private final AtomicLong lastCacheChanged = new AtomicLong();
    /**
     * 已注册 服务URL 集合
     * 注册的 URL 不仅仅可以是服务提供者的，也可以是服务消费者的
     */
    private final Set<URL> registered = new ConcurrentHashSet<URL>();
    /**
     * 消费者url订阅的监听器集合
     */
    private final ConcurrentMap<URL, Set<NotifyListener>> subscribed = new ConcurrentHashMap<URL, Set<NotifyListener>>();
    /**
     * 某个消费者 被通知的 服务URL 集合，并且该服务URL有分类
     *      第一个key是消费者的URL，对应的就是哪个消费者。
     *      value是一个map集合
     *          key是分类（catagory）的意思，例如providers、routes等，
     *          value就是被通知的服务URL集合
     */
    private final ConcurrentMap<URL, Map<String, List<URL>>> notified = new ConcurrentHashMap<URL, Map<String, List<URL>>>();
    /**
     * 注册中心 URL
     * zookeeper://127.0.0.1:2181/com.alibaba.dubbo.registry.RegistryService?application=dubbo-provider-demo&dubbo=2.0.2&file=dubbo-demo/logs/dubbo/cache/dubbo-provider-demo&interface=com.alibaba.dubbo.registry.RegistryService&pid=87978&timestamp=1566731374002
     */
    private URL registryUrl;
    // Local disk cache file
    /**
     * 本地磁盘缓存文件，缓存注册中心的数据
     * dubbo-demo/logs/dubbo/cache/dubbo-provider-demo
     */
    private File file;

    /**
     * url示例：
     *      zookeeper://127.0.0.1:2181/com.alibaba.dubbo.registry.RegistryService?
     *          application=dubbo-provider-demo&dubbo=2.0.2
     *          &file=dubbo-demo/logs/dubbo/cache/dubbo-provider-demo
     *          &interface=com.alibaba.dubbo.registry.RegistryService
     *          &pid=87978&timestamp=1566731374002
     * @param url
     */
    public AbstractRegistry(URL url) {
        //把url放到registryUrl中
        setUrl(url);
        // Start file save timer
        //从url中读取是否同步保存文件的配置，如果没有值默认用异步保存文件
        syncSaveFile = url.getParameter(Constants.REGISTRY_FILESAVE_SYNC_KEY, false);
        //获得file路径
        //保存的文件路径都优先选择URL上的配置，如果没有相关的配置，再选用默认配置
        String filename = url.getParameter(Constants.FILE_KEY, System.getProperty("user.home") + "/.dubbo/dubbo-registry-" + url.getParameter(Constants.APPLICATION_KEY) + "-" + url.getAddress() + ".cache");
        File file = null;
        if (ConfigUtils.isNotEmpty(filename)) {
            //创建文件
            file = new File(filename);
            if (!file.exists() && file.getParentFile() != null && !file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IllegalArgumentException("Invalid registry store file " + file + ", cause: Failed to create directory " + file.getParentFile() + "!");
                }
            }
        }
        this.file = file;
        // 把文件里面的数据写入properties
        loadProperties();
        // 通知监听器，URL 变化结果
        notify(url.getBackupUrls());
    }

    /**
     * 判断url集合是否为空，如果为空，则把url中key为empty的值加入到集合。
     * 该方法只有在notify方法中用到，为了防止通知的URL变化结果为空。
     * @param url
     * @param urls
     * @return
     */
    protected static List<URL> filterEmpty(URL url, List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            List<URL> result = new ArrayList<URL>(1);
            result.add(url.setProtocol(Constants.EMPTY_PROTOCOL));
            return result;
        }
        return urls;
    }

    @Override
    public URL getUrl() {
        return registryUrl;
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("registry url == null");
        }
        this.registryUrl = url;
    }

    public Set<URL> getRegistered() {
        return registered;
    }

    public Map<URL, Set<NotifyListener>> getSubscribed() {
        return subscribed;
    }

    public Map<URL, Map<String, List<URL>>> getNotified() {
        return notified;
    }

    public File getCacheFile() {
        return file;
    }

    public Properties getCacheProperties() {
        return properties;
    }

    public AtomicLong getLastCacheChanged() {
        return lastCacheChanged;
    }
    /**
     * 将集合中的数据存储到文件中
     * @param version
     */
    public void doSaveProperties(long version) {
        // 如果版本号比当前版本老，则不保存
        if (version < lastCacheChanged.get()) {
            return;
        }
        if (file == null) {
            return;
        }
        // Save
        try {
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
            try {
                FileChannel channel = raf.getChannel();
                try {
                    FileLock lock = channel.tryLock();
                    if (lock == null) {
                        throw new IOException("Can not lock the registry cache file " + file.getAbsolutePath() + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.registry.file=xxx.properties");
                    }
                    // Save
                    try {
                        if (!file.exists()) {
                            file.createNewFile();
                        }
                        FileOutputStream outputFile = new FileOutputStream(file);
                        try {
                            // 将集合中的数据存储到文件中，使用store方法
                            properties.store(outputFile, "Dubbo Registry Cache");
                        } finally {
                            outputFile.close();
                        }
                    } finally {
                        lock.release();
                    }
                } finally {
                    channel.close();
                }
            } finally {
                raf.close();
            }
        } catch (Throwable e) {
            if (version < lastCacheChanged.get()) {
                return;
            } else {
                registryCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save registry store file, cause: " + e.getMessage(), e);
        }
    }

    /**
     * 加载本地磁盘缓存文件到内存缓存，也就是把文件里面的数据写入properties
     * 示例：
     * key: com.tree.dubbo.api.DemoService:1.0
     * value: empty://192.168.1.100:20880/com.tree.dubbo.api.DemoService?
     *      anyhost=true&application=dubbo-provider-demo
     *      &bean.name=com.tree.dubbo.api.DemoService
     *      &category=configurators
     *      &check=false
     *      &default.connections=5
     *      &default.retries=0
     *      &default.timeout=10000
     *      &default.version=1.0
     *      &dubbo=2.0.2
     *      &generic=false
     *      &interface=com.tree.dubbo.api.DemoService
     *      &methods=sayHello&pid=87858
     *      &side=provider
     *      &threadpool=fixed
     *      &threads=500
     *      &timestamp=1566731050622
     */
    private void loadProperties() {
        if (file != null && file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                // 把数据写入到内存缓存中
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load registry store file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load registry store file " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
    }
    /**
     * 获得内存缓存properties中相关value，并且返回为一个集合
     * @param url
     * @return
     */
    public List<URL> getCacheUrls(URL url) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            // key为某个分类，例如服务提供者分类
            String key = (String) entry.getKey();
            // value为某个分类的列表，例如服务提供者列表
            String value = (String) entry.getValue();
            if (key != null && key.length() > 0 && key.equals(url.getServiceKey())
                    && (Character.isLetter(key.charAt(0)) || key.charAt(0) == '_')
                    && value != null && value.length() > 0) {
                //分割出列表的每个值
                String[] arr = value.trim().split(URL_SPLIT);
                List<URL> urls = new ArrayList<URL>();
                for (String u : arr) {
                    urls.add(URL.valueOf(u));
                }
                return urls;
            }
        }
        return null;
    }

    @Override
    public List<URL> lookup(URL url) {
        List<URL> result = new ArrayList<URL>();
        // 获得该消费者url订阅的 所有被通知的 服务URL集合
        Map<String, List<URL>> notifiedUrls = getNotified().get(url);
        // 判断该消费者是否订阅服务
        if (notifiedUrls != null && notifiedUrls.size() > 0) {
            for (List<URL> urls : notifiedUrls.values()) {
                for (URL u : urls) {
                    // 判断协议是否为空
                    if (!Constants.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        // 添加 该消费者订阅的服务URL
                        result.add(u);
                    }
                }
            }
        } else {
            // 原子类 避免在获取注册在注册中心的服务url时能够保证是最新的url集合
            final AtomicReference<List<URL>> reference = new AtomicReference<List<URL>>();
            // 通知监听器。当收到服务变更通知时触发
            NotifyListener listener = new NotifyListener() {
                @Override
                public void notify(List<URL> urls) {
                    reference.set(urls);
                }
            };
            // 订阅服务，就是消费者url订阅已经 注册在注册中心的服务（也就是添加该服务的监听器）
            subscribe(url, listener); // Subscribe logic guarantees the first notify to return
            List<URL> urls = reference.get();
            if (urls != null && !urls.isEmpty()) {
                for (URL u : urls) {
                    if (!Constants.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void register(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("register url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Register: " + url);
        }
        registered.add(url);
    }

    @Override
    public void unregister(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("unregister url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unregister: " + url);
        }
        registered.remove(url);
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("subscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Subscribe: " + url);
        }
        // 获得该消费者url 已经订阅的服务 的监听器集合
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners == null) {
            subscribed.putIfAbsent(url, new ConcurrentHashSet<NotifyListener>());
            listeners = subscribed.get(url);
        }
        // 添加某个服务的监听器
        listeners.add(listener);
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("unsubscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unsubscribe: " + url);
        }
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }
    /**
     *  恢复，在注册中心断开，重连成功的时候，会恢复注册和订阅
     * @throws Exception
     */
    protected void recover() throws Exception {
        // register
        //把内存缓存中的registered取出来遍历进行注册
        Set<URL> recoverRegistered = new HashSet<URL>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (URL url : recoverRegistered) {
                register(url);
            }
        }
        // subscribe
        //把内存缓存中的subscribed取出来遍历进行订阅
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    subscribe(url, listener);
                }
            }
        }
    }
    /**
     * 遍历监听器，通知监听器,URL 变化
     * zookeeper://127.0.0.1:2181/com.alibaba.dubbo.registry.RegistryService
     *      ?application=dubbo-provider-demo
     *      &dubbo=2.0.2
     *      &file=dubbo-demo/logs/dubbo/cache/dubbo-provider-demo
     *      &interface=com.alibaba.dubbo.registry.RegistryService
     *      &pid=87978
     *      &timestamp=1566731374002
     * @param urls
     */
    protected void notify(List<URL> urls) {
        if (urls == null || urls.isEmpty()) return;
        // 遍历订阅URL的监听器集合，通知他们
        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {
            URL url = entry.getKey();
            // 匹配
            if (!UrlUtils.isMatch(url, urls.get(0))) {
                continue;
            }

            // 遍历监听器集合，通知他们
            Set<NotifyListener> listeners = entry.getValue();
            if (listeners != null) {
                for (NotifyListener listener : listeners) {
                    try {
                        notify(url, listener, filterEmpty(url, urls));
                    } catch (Throwable t) {
                        logger.error("Failed to notify registry event, urls: " + urls + ", cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }
    /**
     * 通知监听器，URL 变化结果
     * @param url
     * @param listener
     * @param urls
     */
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((urls == null || urls.isEmpty())
                && !Constants.ANY_VALUE.equals(url.getServiceInterface())) {
            logger.warn("Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify urls for subscribe url " + url + ", urls: " + urls);
        }
        Map<String, List<URL>> result = new HashMap<String, List<URL>>();
        // 将urls进行分类
        for (URL u : urls) {
            if (UrlUtils.isMatch(url, u)) {
                // 按照url中key为category对应的值进行分类，如果没有该值，就找key为providers的值进行分类
                String category = u.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
                List<URL> categoryList = result.get(category);
                if (categoryList == null) {
                    categoryList = new ArrayList<URL>();
                    // 分类结果放入result
                    result.put(category, categoryList);
                }
                categoryList.add(u);
            }
        }
        if (result.size() == 0) {
            return;
        }
        // 获得某一个消费者被通知的url集合（通知的 URL 变化结果）
        Map<String, List<URL>> categoryNotified = notified.get(url);
        if (categoryNotified == null) {
            // 添加该消费者对应的url
            notified.putIfAbsent(url, new ConcurrentHashMap<String, List<URL>>());
            categoryNotified = notified.get(url);
        }
        // 处理通知监听器URL 变化结果
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();
            // 把分类标实和分类后的列表放入notified的value中
            // 覆盖到 `notified`
            // 当某个分类的数据为空时，会依然有 urls 。其中 `urls[0].protocol = empty` ，通过这样的方式，处理所有服务提供者为空的情况。
            categoryNotified.put(category, categoryList);
            // 保存到文件
            saveProperties(url);
            //通知监听器
            listener.notify(categoryList);
        }
    }

    /**
     * 单个消费者url对应在notified中的数据，保存在到文件
     * @param url
     */
    private void saveProperties(URL url) {
        if (file == null) {
            return;
        }

        try {
            // 拼接url
            StringBuilder buf = new StringBuilder();
            Map<String, List<URL>> categoryNotified = notified.get(url);
            if (categoryNotified != null) {
                for (List<URL> us : categoryNotified.values()) {
                    for (URL u : us) {
                        if (buf.length() > 0) {
                            buf.append(URL_SEPARATOR);
                        }
                        buf.append(u.toFullString());
                    }
                }
            }
            // 设置到properties中
            properties.setProperty(url.getServiceKey(), buf.toString());
            // 增加版本号
            long version = lastCacheChanged.incrementAndGet();
            if (syncSaveFile) {
                // 将集合中的数据存储到文件中
                doSaveProperties(version);
            } else {
                //异步开启保存到文件
                registryCacheExecutor.execute(new SaveProperties(version));
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    /**
     * 当JVM关闭时，取消注册和订阅
     */
    @Override
    public void destroy() {
        if (logger.isInfoEnabled()) {
            logger.info("Destroy registry:" + getUrl());
        }
        Set<URL> destroyRegistered = new HashSet<URL>(getRegistered());
        if (!destroyRegistered.isEmpty()) {
            for (URL url : new HashSet<URL>(getRegistered())) {
                if (url.getParameter(Constants.DYNAMIC_KEY, true)) {
                    try {
                        unregister(url);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unregister url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unregister url " + url + " to registry " + getUrl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
        Map<URL, Set<NotifyListener>> destroySubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!destroySubscribed.isEmpty()) {
            for (Map.Entry<URL, Set<NotifyListener>> entry : destroySubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    try {
                        unsubscribe(url, listener);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unsubscribe url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unsubscribe url " + url + " to registry " + getUrl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return getUrl().toString();
    }

    private class SaveProperties implements Runnable {
        private long version;

        private SaveProperties(long version) {
            this.version = version;
        }

        @Override
        public void run() {
            doSaveProperties(version);
        }
    }

}
