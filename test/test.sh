#!/bin/sh -x 

JAVACMD="java -cp ../target/rados4j.jar:. -Djava.library.path=../target/ Test"
javac -cp ../target/rados4j.jar:. Test.java
if [ $? -ne 0 ]; then
    echo "compile failed"
    exit 1
fi

#dd if=/dev/zero of=./in.img bs=1M count=1024
#$JAVACMD putObj test3 in.img in.img
#$JAVACMD getObjDirect test3 in.img out.img
#$JAVACMD getObj test3 in.img out.img
#
#exit 1

rados rmpool hoge1
rados rmpool hoge2
rm -f tmp1 tmp2 tmp3
$JAVACMD createBucket hoge1
if [ $? -ne 0 ]; then
	echo "createBucket failed"
	exit 1
fi
FOUND=`rados lspools | grep hoge1 | wc -l`
if [ $FOUND -eq 0 ]; then
	echo "createBucket failed"
	exit 1
fi
$JAVACMD createBucket hoge1
if [ $? -eq 0 ]; then
	echo "createBucket failed"
	exit 1
fi
$JAVACMD lookupBucket hoge1
if [ $? -ne 0 ]; then
	echo "lookupBucket failed"
	exit 1
fi
$JAVACMD deleteBucket hoge1
if [ $? -ne 0 ]; then
	echo "deleteBucket failed"
	exit 1
fi
FOUND=`rados lspools | grep hoge1 | wc -l`
if [ $FOUND -ne 0 ]; then
	echo "createBucket failed"
	exit 1
fi
$JAVACMD lookupBucket hoge1
if [ $? -eq 0 ]; then
	echo "lookupBucket failed"
	exit 1
fi
$JAVACMD deleteBucket hoge1
if [ $? -eq 0 ]; then
	echo "deleteBucket failed"
	exit 1
fi
rados mkpool hoge1
echo 123 > tmp1
$JAVACMD putObj hoge1 tmp1 tmp1
if [ $? -ne 4 ]; then
	echo "putObj failed"
	exit 1
fi
rados rmpool hoge1
$JAVACMD putObj hoge1 tmp1 tmp1
if [ $? -eq 4 ]; then
	echo "putObj failed"
	exit 1
fi

rados mkpool hoge1
$JAVACMD putObj hoge1 tmp1 tmp1
if [ $? -ne 4 ]; then
	echo "putObj failed"
	exit 1
fi

$JAVACMD getObj hoge1 tmp1 tmp2
if [ $? -ne 4 ]; then
	echo "getObj failed"
	exit 1
fi
diff tmp1 tmp2
if [ $? -ne 0 ]; then
    echo "getObj diff failed"
    exit 1
fi
$JAVACMD getObj hoge1 tmp3 tmp3
if [ $? -eq 4 ]; then
	echo "getObj failed"
	exit 1
fi
$JAVACMD getObj hoge2 tmp3 tmp3
if [ $? -eq 4 ]; then
	echo "getObj failed"
	exit 1
fi

#rados rmpool hoge1
#rados mkpool hoge1
#echo 123 > tmp1
#$JAVACMD putObjDirect hoge1 tmp1 tmp1
#if [ $? -ne 4 ]; then
#	echo "putObjDirect failed"
#	exit 1
#fi
#rados rmpool hoge1
#$JAVACMD putObjDirect hoge1 tmp1 tmp1
#if [ $? -eq 4 ]; then
#	echo "putObjDirect failed"
#	exit 1
#fi
#
#rados mkpool hoge1
#$JAVACMD putObjDirect hoge1 tmp1 tmp1
#if [ $? -ne 4 ]; then
#	echo "putObjDirect failed"
#	exit 1
#fi
#
#$JAVACMD getObjDirect hoge1 tmp1 tmp2
#if [ $? -ne 4 ]; then
#	echo "getObjDirect failed"
#	exit 1
#fi
#diff tmp1 tmp2
#if [ $? -ne 0 ]; then
#    echo "getObjDirect diff failed"
#    exit 1
#fi
#$JAVACMD getObjDirect hoge1 tmp3 tmp3
#if [ $? -eq 4 ]; then
#	echo "getObjDirect failed"
#	exit 1
#fi
#$JAVACMD getObjDirect hoge2 tmp3 tmp3
#if [ $? -eq 4 ]; then
#	echo "getObjDirect failed"
#	exit 1
#fi


$JAVACMD deleteObj hoge1 tmp1
if [ $? -ne 0 ]; then
	echo "deleteObj failed"
	exit 1
fi
$JAVACMD deleteObj hoge1 tmp2
if [ $? -ne 0 ]; then
	echo "deleteObj failed"
	exit 1
fi
$JAVACMD deleteObj hoge2 tmp1
if [ $? -eq 0 ]; then
	echo "deleteObj failed"
	exit 1
fi

$JAVACMD putObj hoge1 tmp1 tmp1
if [ $? -ne 4 ]; then
	echo "putObj failed"
	exit 1
fi
$JAVACMD stat hoge1 tmp1
if [ $? -ne 0 ]; then
	echo "stat failed"
	exit 1
fi

$JAVACMD listObj hoge1
if [ $? -ne 0 ]; then
	echo "listObj failed"
	exit 1
fi

$JAVACMD listObj hoge2
if [ $? -eq 0 ]; then
	echo "listObj failed"
	exit 1
fi

$JAVACMD listPools
if [ $? -ne 0 ]; then
	echo "listPools failed"
	exit 1
fi

rm -fv tmp* *.class
