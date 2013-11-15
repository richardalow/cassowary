package com.wentnet.cassowary.sstable;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class SSTableInputFormat implements InputFormat<MapWritable, MapWritable> {

	private static final Log logger = LogFactory.getLog(SSTableInputFormat.class);

	private final InputFormat<MapWritable, MapWritable> baseInputFormat;

	/**
	 * This is hideous. Cassandra and Hive 0.9 use mutually incompatible Antlr
	 * and Guava versions. This hack loads stuff we need in a separate class
	 * loader, forcibly getting the classes from the cassowary jar.
	 * 
	 */
	public static class JarClassLoader extends ClassLoader {
		private final String jarFile;
		private final Map<String, Class<?>> classes = new HashMap<String, Class<?>>();

		public JarClassLoader(String jarFile) throws IOException {
			super(SSTableInputFormat.class.getClassLoader());
			this.jarFile = jarFile;
		}

		@Override
		public URL getResource(String name) {
			logger.trace("getResource: " + name);
			return JarClassLoader.class.getClassLoader().getResource(name);
		}

		@Override
		public InputStream getResourceAsStream(String name) {
			logger.trace("Loading resource " + name);
			return JarClassLoader.class.getClassLoader().getResourceAsStream(name);
		}

		@Override
		public Class<?> loadClass(String className) throws ClassNotFoundException {
			logger.debug("Finding class " + className);
			if (!className.startsWith("org.antlr.")) {
				final Class<?> loadedClass = findLoadedClass(className);
				if (loadedClass != null)
					return loadedClass;
			}

			if (className.startsWith("org.apache.log4j") || className.startsWith("org.slf4j") || className.equals(SSTableSplit.class.getName())
					|| className.startsWith("org.xerial.snappy"))
				return JarClassLoader.class.getClassLoader().loadClass(className);

			try {
				return loadClassFromJar(className);
			} catch (ClassNotFoundException e) {
				logger.debug("Couldn't find class " + className + ", reverting to default parent class loader");
				return JarClassLoader.class.getClassLoader().loadClass(className);
			}
		}

		private Class<?> loadClassFromJar(String className) throws ClassNotFoundException {
			Class<?> result = null;

			result = classes.get(className); // checks in cached classes
			if (result != null) {
				return result;
			}

			try {
				final JarFile jar = new JarFile(jarFile);
				try {
					final String path = className.replace('.', '/');
					final JarEntry entry = jar.getJarEntry(path + ".class");
					final InputStream in = jar.getInputStream(entry);
					try {
						final byte[] classBytes = IOUtils.toByteArray(in);
						result = defineClass(className, classBytes, 0, classBytes.length, null);
						classes.put(className, result);
						return result;
					} finally {
						in.close();
					}
				} finally {
					jar.close();
				}
			} catch (Exception e) {
				throw new ClassNotFoundException("Can't find class " + className + " in jar " + jarFile, e);
			}
		}

		@Override
		public String toString() {
			return "JarClassLoader from jar " + jarFile;
		}
	}

	private static ClassLoader jarLoader;
	static {
		try {
			// find the jar that contains SSTableInputFormatImpl
			final URL classURL = SSTableInputFormat.class.getResource("/" + SSTableInputFormatImpl.class.getName().replace('.', '/') + ".class");
			String jarURL = classURL.toString().split("\\!")[0];
			final int lastColon = jarURL.lastIndexOf(':');
			if (lastColon >= 0)
				jarURL = jarURL.substring(lastColon + 1);
			logger.info("Using jar file " + jarURL);
			jarLoader = new JarClassLoader(jarURL);
		} catch (IOException e) {
			logger.error("Can't load JarClassLoader", e);
		}
	}

	@SuppressWarnings("unchecked")
	public SSTableInputFormat() throws IOException {
		try {
			final Class<InputFormat<MapWritable, MapWritable>> clazz = (Class<InputFormat<MapWritable, MapWritable>>) jarLoader
					.loadClass(SSTableInputFormatImpl.class.getName());
			final Constructor<InputFormat<MapWritable, MapWritable>> constructor = clazz.getConstructor();
			baseInputFormat = constructor.newInstance();
		} catch (ClassNotFoundException e) {
			logger.error("Can't find class", e);
			throw new IOException(e);
		} catch (SecurityException e) {
			logger.error("Can't access class", e);
			throw new IOException(e);
		} catch (NoSuchMethodException e) {
			logger.error("Can't find constructor", e);
			throw new IOException(e);
		} catch (IllegalArgumentException e) {
			logger.error("Illegal argument", e);
			throw new IOException(e);
		} catch (InstantiationException e) {
			logger.error("Can't instantiate class", e);
			throw new IOException(e);
		} catch (IllegalAccessException e) {
			logger.error("Illegal access to class", e);
			throw new IOException(e);
		} catch (InvocationTargetException e) {
			logger.error("Error calling constructor", e);
			throw new IOException(e);
		}
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		return baseInputFormat.getSplits(job, numSplits);
	}

	@Override
	public RecordReader<MapWritable, MapWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return baseInputFormat.getRecordReader(split, job, reporter);
	}
}
