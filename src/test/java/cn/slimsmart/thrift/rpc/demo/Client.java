package cn.slimsmart.thrift.rpc.demo;

import cn.slimsmart.thrift.rpc.ThriftServiceClientProxyFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;
import java.util.Map.Entry;

//客户端调用
@SuppressWarnings("resource")
public class Client {
	public static void main(String[] args) {
		//simple();
		spring();
	}

//	@Test
	public static void spring() {
		try {
			final ApplicationContext context = new ClassPathXmlApplicationContext("spring-context-thrift-client.xml");
			EchoSerivce.Iface echoSerivce = (EchoSerivce.Iface) context.getBean("echoSerivce");
			System.out.println(echoSerivce.echo("hello--echo"));
			ThriftClientMultipleServiceProxyFactory serviceProxies = (ThriftClientMultipleServiceProxyFactory)context.getBean("serviceProxies");
			System.out.println(serviceProxies.invoke("cn.slimsmart.thrift.rpc.demo.EchoSerivce", "echo",
					new String[]{"hello world, I am lovely service proxy"}));

			//关闭连接的钩子
			Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                	Map<String,ThriftServiceClientProxyFactory>
                	clientMap = context.getBeansOfType(ThriftServiceClientProxyFactory.class);
                	for(Entry<String, ThriftServiceClientProxyFactory> client : clientMap.entrySet()){
                		System.out.println("serviceName : "+client.getKey() + ",class obj: "+client.getValue());
                		client.getValue().close();
                	}
                }
            });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static class TThread extends Thread {
		EchoSerivce.Iface echoSerivce;
		TThread(EchoSerivce.Iface service) {
			echoSerivce = service;
		}
		public void run() {
			try {
				for (int i = 0; i < 10; i++) {
					Thread.sleep(1000*i);
					System.out.println(Thread.currentThread().getName()+"  "+echoSerivce.echo("hello"));
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void simple() {
		try {
			TSocket socket = new TSocket("192.168.36.215", 9001);
			TTransport transport = new TFramedTransport(socket);
			TProtocol protocol = new TBinaryProtocol(transport);
			EchoSerivce.Client client = new EchoSerivce.Client(protocol);
			transport.open();
			System.out.println(client.echo("helloword"));
			Thread.sleep(3000);
			transport.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
