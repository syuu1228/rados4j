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
#include "com_dokukino_rados4j_ListCtx.h"
#include <rados/librados.hpp>
#include <list>

jclass list_ctx_class = NULL;
jmethodID list_ctx_constructor = NULL;
jfieldID list_ctx_instance_ptr = NULL, list_ctx_rados_ptr = NULL;

int
list_ctx_field_initialize(JNIEnv *env)
{
    jclass clsj = env->FindClass("Lcom/dokukino/rados4j/ListCtx;");
    if (!clsj)
        return -1;
    list_ctx_class = reinterpret_cast<jclass>(env->NewGlobalRef(clsj));
    env->DeleteLocalRef(clsj);
    list_ctx_constructor = env->GetMethodID(list_ctx_class, "<init>", "()V");
    if (!list_ctx_constructor)
        return -1;
    list_ctx_instance_ptr = env->GetFieldID(list_ctx_class, "instancePtr", "J");
    if (!list_ctx_instance_ptr)
        return -1;
    list_ctx_rados_ptr = env->GetFieldID(list_ctx_class, "radosPtr", "J");
    if (!list_ctx_rados_ptr)
        return -1;
    return 0;
}

static inline void
list_ctx_set_instance_ptr(JNIEnv *env, jobject obj, librados::Rados::ListCtx *ptr)
{
    env->SetLongField(obj, list_ctx_instance_ptr, reinterpret_cast<long>(ptr));
}

static inline librados::Rados::ListCtx *
list_ctx_get_instance_ptr(JNIEnv *env, jobject obj)
{
    jlong ptr = env->GetLongField(obj, list_ctx_instance_ptr);
    return reinterpret_cast<librados::Rados::ListCtx *>(ptr);
}

static inline librados::Rados *
list_ctx_get_rados_ptr(JNIEnv *env, jobject obj)
{
    jlong ptr = env->GetLongField(obj, list_ctx_rados_ptr);
    return reinterpret_cast<librados::Rados *>(ptr);
}

extern "C"
{
    /*
     * Class:     com_dokukino_rados4j_ListCtx
     * Method:    more
     * Signature: (I[Ljava/lang/String;)I
     */
    JNIEXPORT jint JNICALL Java_com_dokukino_rados4j_ListCtx_more
    (JNIEnv *env, jobject obj, jint max, jobjectArray entries)
    {
        librados::Rados::ListCtx *ctx;
        librados::Rados *rados;
        std::list<std::string> entries_list;
        jint r;

        if (!entries)
            return -1;
        ctx = list_ctx_get_instance_ptr(env, obj);
        if (!ctx)
            return -1;
        rados = list_ctx_get_rados_ptr(env, obj);
        if (!rados)
            return -1;
        r = rados->list_objects_more(*ctx, max, entries_list);
        if (r < 0)
            return r;
        int i = 0;
        const int len = env->GetArrayLength(entries);
        for (std::list<std::string>::iterator iter = entries_list.begin();
                i < len && iter != entries_list.end(); ++iter, ++i)
        {
            jstring entry = env->NewStringUTF((*iter).c_str());
            env->SetObjectArrayElement(entries, i, entry);
        }

        return r;
    }

    /*
     * Class:     com_dokukino_rados4j_ListCtx
     * Method:    close
     * Signature: ()V
     */
    JNIEXPORT void JNICALL Java_com_dokukino_rados4j_ListCtx_close
    (JNIEnv *env, jobject obj)
    {
        librados::Rados::ListCtx *ctx;
        librados::Rados *rados;

        ctx = list_ctx_get_instance_ptr(env, obj);
        if (!ctx)
            return;
        rados = list_ctx_get_rados_ptr(env, obj);
        if (!rados)
            return;
        rados->list_objects_close(*ctx);
        delete ctx;
	list_ctx_set_instance_ptr(env, obj, NULL);
    }

}
