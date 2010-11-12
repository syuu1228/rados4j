/*******************************************************************************
*Copyright (c) 2010  Takuya ASADA
* 
*  This program is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, only version 3 of the License.
* 
* 
*  This file is distributed in the hope that it will be useful, but WITHOUT
*  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
*  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
*  for more details.
* 
*  You should have received a copy of the GNU General Public License along
*  with this program.  If not, see <http://www.gnu.org/licenses/>.
* 
*  Please contact Eucalyptus Systems, Inc., 130 Castilian
*  Dr., Goleta, CA 93101 USA or visit <http://www.eucalyptus.com/licenses/>
*  if you need additional information or have any questions.
* 
*  This file may incorporate work covered under the following copyright and
*  permission notice:
* 
*    Software License Agreement (BSD License)
* 
*    Copyright (c) 2008, Regents of the University of California
*    All rights reserved.
* 
*    Redistribution and use of this software in source and binary forms, with
*    or without modification, are permitted provided that the following
*    conditions are met:
* 
*      Redistributions of source code must retain the above copyright notice,
*      this list of conditions and the following disclaimer.
* 
*      Redistributions in binary form must reproduce the above copyright
*      notice, this list of conditions and the following disclaimer in the
*      documentation and/or other materials provided with the distribution.
* 
*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
*    IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
*    TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
*    PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
*    OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
*    EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
*    PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
*    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
*    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
*    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
*    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. USERS OF
*    THIS SOFTWARE ACKNOWLEDGE THE POSSIBLE PRESENCE OF OTHER OPEN SOURCE
*    LICENSED MATERIAL, COPYRIGHTED MATERIAL OR PATENTED MATERIAL IN THIS
*    SOFTWARE, AND IF ANY SUCH MATERIAL IS DISCOVERED THE PARTY DISCOVERING
*    IT MAY INFORM DR. RICH WOLSKI AT THE UNIVERSITY OF CALIFORNIA, SANTA
*    BARBARA WHO WILL THEN ASCERTAIN THE MOST APPROPRIATE REMEDY, WHICH IN
*    THE REGENTS’ DISCRETION MAY INCLUDE, WITHOUT LIMITATION, REPLACEMENT
*    OF THE CODE SO IDENTIFIED, LICENSING OF THE CODE SO IDENTIFIED, OR
*    WITHDRAWAL OF THE CODE CAPABILITY TO THE EXTENT NEEDED TO COMPLY WITH
*    ANY SUCH LICENSES OR RIGHTS.
*******************************************************************************/
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import com.dokukino.rados4j.*;
import java.util.*;

public class Test {	
    public static void main(String[] args) throws Exception {
		int r;
		Rados rados = new Rados();
		r = rados.initialize(new String[]{"-c", "/etc/ceph/ceph.conf1"});
		if (r < 0) {
			System.out.println("initialize failed");
			rados.shutdown();
			System.exit(r);
		}
		
		if (args[0].equals("deleteBucket")) {
			Pool pool = null;
		
			pool = rados.openPool(args[1]);
			if (pool == null) {
			    rados.shutdown();
			    System.exit(-1);
			}
			r = pool.delete();
			System.out.printf("deletePool:%d\n", r);
			pool.close();
		} else if(args[0].equals("createBucket")) {
			r = rados.createPool(args[1]);
			System.out.printf("createPool:%d\n", r);
 		} else if(args[0].equals("lookupBucket")) {
			boolean res = rados.lookupPool(args[1]);
			r = res ? 0 : -1;
			System.out.printf("lookupPool:%b\n", res);
		} else if(args[0].equals("putObj")) {
			File file = new File(args[3]);
			byte[] buf = new byte[(int)file.length()];
			InputStream is = new FileInputStream(file);
			Pool pool = null;
			int len;
			
			len = is.read(buf, 0, (int)file.length());
			is.close();
			pool = rados.openPool(args[1]);
			if (pool == null) {
			    rados.shutdown();
			    System.exit(-1);
			}
			double start = (double)System.currentTimeMillis()/1000;

			r = pool.writeObj(args[2], 0, buf, len);

			double end = (double)System.currentTimeMillis()/1000;
			double elapsed = end - start;
			System.out.printf("size:%f\n", ((double)r/(double)(1024*1024)));
			System.out.printf("write %fsec, %f MB/s\n", elapsed,
				((double)r/(double)(1024*1024))/elapsed);
			System.out.printf("writeObj:%d\n", r);
			pool.close();

		} else if(args[0].equals("putMulti")) {
			class PutThread extends Thread {
				private Rados rados;
				private String poolName, oid;
				private File file;
				
				public PutThread(Rados rados, String poolName, String oid, String fileName) {
					this.poolName = poolName;
					this.rados = rados;
					this.oid = oid;
					this.file = new File(fileName);
				}
				
				public void run() {
					try {
						byte[] buf = new byte[(int)file.length()];
						InputStream is = new FileInputStream(file);
						Pool pool = null;
						int len;
						int r;
			
						len = is.read(buf, 0, (int)file.length());
						is.close();
						pool = rados.openPool(poolName);
						if (pool == null)
							return;
						System.out.printf("[%s] start writeObj(%s)\n", new Date(), oid);
						r = pool.writeObj(oid, 0, buf, len);
						System.out.printf("[%s] end writeObj(%s)\n", new Date(), oid);
						pool.close();
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			};

			PutThread threads[] = new PutThread[10];
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new PutThread(rados, args[1], args[2] + i, args[3]);
				threads[i].start();
				System.out.printf("thread%d started\n", i);
			}
			for (int i = 0; i < threads.length; i++) {
				threads[i].join();
				System.out.printf("thread%d finished\n", i);
			}
			/*
		} else if(args[0].equals("putObjDirect")) {
			FileChannel channel = new FileInputStream(args[3]).getChannel();
			ByteBuffer buf = ByteBuffer.allocateDirect(1000000);
			Pool pool = null;
			int len;

			len = channel.read(buf);
			channel.close();
			pool = rados.openPool(args[1]);
			if (pool == null) {
			    rados.shutdown();
			    System.exit(-1);
			}
			r = pool.writeObjDirect(args[2], 0, buf, len);
			System.out.printf("writeObj:%d\n", r);
			pool.close();
			*/
		} else if(args[0].equals("getObj")) {
			byte[] buf = null;
			Pool pool = null;
			Stat stat = null;
			

			pool = rados.openPool(args[1]);
			if (pool == null) {
			    rados.shutdown();
			    System.exit(-1);
			}
			stat = pool.statObj(args[2]);
			if (stat == null) {
				pool.close();
				rados.shutdown();
				System.exit(-1);
			}
			buf = new byte[(int)stat.getSize()];
			double start = (double)System.currentTimeMillis()/1000;
			r = pool.readObj(args[2], 0, buf, 0);

			double end = (double)System.currentTimeMillis()/1000;
			double elapsed = end - start;
			System.out.printf("size:%f\n", ((double)r/(double)(1024*1024)));
			System.out.printf("read %fsec, %f MB/s\n", elapsed,
				((double)r/(double)(1024*1024))/elapsed);
			OutputStream os = new FileOutputStream(args[3]);
			os.write(buf, 0, (int)stat.getSize());
			os.close();
			pool.close();
/*
		} else if(args[0].equals("getObjDirect")) {
			ByteBuffer buf = null;
			Pool pool = null;
			Stat stat = null;
		
			pool = rados.openPool(args[1]);
			if (pool == null) {
			System.out.println("pool not found");
			    rados.shutdown();
			    System.exit(-1);
			}
			stat = pool.statObj(args[2]);
			if (stat == null) {
			System.out.println("obj not found");
				pool.close();
				rados.shutdown();
				System.exit(-1);
			}
			buf = ByteBuffer.allocateDirect((int)stat.getSize()); 
			double start = (double)System.currentTimeMillis()/1000;
			r = pool.readObjDirect(args[2], 0, buf, stat.getSize());

			double end = (double)System.currentTimeMillis()/1000;
			double elapsed = end - start;
			System.out.printf("size:%f\n", ((double)r/(double)(1024*1024)));
			System.out.printf("read %fsec, %f MB/s\n", elapsed,
				((double)r/(double)(1024*1024))/elapsed);
			buf.limit(r);
			FileChannel channel = new FileOutputStream(args[3]).getChannel();
			channel.write(buf);
			channel.close();
			pool.close();
*/
		} else if(args[0].equals("deleteObj")) {
			Pool pool = null;
			
			pool = rados.openPool(args[1]);
			if (pool == null) {
			    rados.shutdown();
			    System.exit(-1);
			}
			r = pool.removeObj(args[2]);
			pool.close();
		} else if(args[0].equals("stat")) {
			Pool pool = null;
			Stat stat = null;
			
			pool = rados.openPool(args[1]);
			if (pool == null) {
			    rados.shutdown();
			    System.exit(-1);
			}
			stat = pool.statObj(args[2]);
			if (stat == null) {
				pool.close();
				rados.shutdown();
			    System.exit(-1);
			}
			System.out.printf("size:%d time:%d\n", stat.getSize(),
							  stat.getMtime());
			pool.close();
 		} else if(args[0].equals("listPools")) {
			String[] entries = new String[128];

			r = rados.listPools(entries);

			if (r >= 0)
				for(int i = 0; entries[i] != null && i < entries.length; i++)
					System.out.printf("entries[%d]: %s\n", i, entries[i]);
		} else if(args[0].equals("listObj")) {
			String[] entries = new String[128];
			Pool pool = null;
			ListCtx ctx = null;

			pool = rados.openPool(args[1]);
			if (pool == null) {
			    rados.shutdown();
			    System.exit(-1);
			}
			ctx = pool.openList();
			if (ctx == null) {
			    rados.shutdown();
			    System.exit(-1);
			}
			r = ctx.more(128, entries);
			ctx.close();
			pool.close();
			if (r >= 0)
				for(int i = 0; entries[i] != null && i < entries.length; i++)
					System.out.printf("entries[%d]: %s\n", i, entries[i]);
		}

		rados.shutdown();
		System.exit(r);
    }
}
