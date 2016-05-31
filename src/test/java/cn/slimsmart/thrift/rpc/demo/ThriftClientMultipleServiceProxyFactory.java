package cn.slimsmart.thrift.rpc.demo;

import cn.slimsmart.thrift.rpc.ThriftServiceClientProxyFactory;
import cn.slimsmart.thrift.rpc.zookeeper.ThriftServerAddressProvider;
import org.springframework.beans.factory.InitializingBean;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhouyanhui on 16-5-31.
 */
public class ThriftClientMultipleServiceProxyFactory implements InitializingBean,Closeable {
      /**
       * 设置属性
       */
      // 所有的service 全名列表
//      private List<String> services;
      private Integer maxActive = 32;// 最大活跃连接数
      // ms,default 3 min,链接空闲时间
      // -1,关闭空闲检测
      private Integer idleTime = 180000;

      private List<ThriftServerAddressProvider> serverAddressProviders;

//      public void setServices(List<String> services) {
//            this.services = services;
//      }

      public void setMaxActive(Integer maxActive) {
            this.maxActive = maxActive;
      }

      public void setIdleTime(Integer idleTime) {
            this.idleTime = idleTime;
      }

      /**
       * 内部属性
       */
      private Map<String, ThriftServiceClientProxyFactory> _proxyMap;

      /**
       * service 全名
       * @param service
       * @return
       */
      public Object getProxy(String service) throws Exception {
            ThriftServiceClientProxyFactory thriftClientServiceProxyFactory = _proxyMap.get(service);
            if(thriftClientServiceProxyFactory == null){
                  return null;
            }
            return thriftClientServiceProxyFactory.getObject();
      }

      /**
       * 使用代理反射调用
       *
       * @param service
       * @param method
       * @param parameters
       * @return
       * @throws Exception
       */
      public Object invoke(String service, String method, Object[] parameters) throws Exception {
            ThriftServiceClientProxyFactory thriftClientServiceProxyFactory = _proxyMap.get(service);
            if(thriftClientServiceProxyFactory == null){
                  throw new RuntimeException("no proxy factory for service: " + service);
            }
            Object proxy = getProxy(service);
            Class<?> objectType = thriftClientServiceProxyFactory.getObjectType();
            Class<?>[] parameterTypes = new Class[parameters.length];
            for (int i = 0; i < parameters.length; i++) {
                  parameterTypes[i] = parameters[i].getClass();
            }
            Method invokeMethod = objectType.getMethod(method, parameterTypes);
            return invokeMethod.invoke(proxy, parameters);
      }

      @Override
      public void close() throws IOException {
            for (ThriftServiceClientProxyFactory thriftClientServiceProxyFactory :
                        _proxyMap.values()) {
                  thriftClientServiceProxyFactory.close();
            }
      }

      @Override
      public void afterPropertiesSet() throws Exception {
            _proxyMap = new HashMap<String, ThriftServiceClientProxyFactory>(serverAddressProviders.size());
            for (ThriftServerAddressProvider serverAddressProvider : serverAddressProviders) {
                  ThriftServiceClientProxyFactory thriftClientServiceProxyFactory =
                              new ThriftServiceClientProxyFactory();
                  thriftClientServiceProxyFactory.setIdleTime(idleTime);
                  thriftClientServiceProxyFactory.setMaxActive(maxActive);
                  thriftClientServiceProxyFactory.setServerAddressProvider(serverAddressProvider);
                  thriftClientServiceProxyFactory.afterPropertiesSet();
                  _proxyMap.put(serverAddressProvider.getService(), thriftClientServiceProxyFactory);
            }
      }


      public List<ThriftServerAddressProvider> getServerAddressProviders() {
            return serverAddressProviders;
      }

      public void setServerAddressProviders(List<ThriftServerAddressProvider> serverAddressProviders) {
            this.serverAddressProviders = serverAddressProviders;
      }
}
