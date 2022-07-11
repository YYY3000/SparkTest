将 spark-redis jar包放入maven仓库:
    mvn install:install-file -Dfile=spark-redis-0.3.2.jar -DgroupId=com.redislabs -DartifactId=spark-redis -Dversion=0.3.2 -Dpackaging=jar