#!/bin/bash

#
# StreamTeam
# Copyright (C) 2019  University of Basel
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#http://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

MASTER=10.34.58.65

cd $DIR

export HADOOP_CONF_DIR=deploy/hadoopConfig
rm -rf target/
rm -rf deploy
mvn clean package
mkdir -p deploy

tar -xf ./target/streamteam-data-stream-analysis-system-1.1.0-dist.tar.gz -C deploy
./runHDFSFileUploader.sh $MASTER:8020 target/streamteam-data-stream-analysis-system-1.1.0-dist.tar.gz input/streamteam-data-stream-analysis-system-1.1.0-dist.tar.gz
./runHDFSFileUploader.sh $MASTER:8020 deploy/config/StreamTeam.properties input/config/StreamTeam.properties

#https://stackoverflow.com/questions/12316167/does-linux-shell-support-list-data-structure
workerConfigFiles=("football/FieldObjectStateGenerationApplication.properties" "football/KickoffDetectionApplication.properties" "football/TimeApplication.properties" "football/BallPossessionApplication.properties" "football/KickDetectionApplication.properties" "football/PassAndShotDetectionApplication.properties" "football/OffsideApplication.properties" "football/HeatmapApplication.properties" "football/AreaDetectionApplication.properties" "football/SetPlayDetectionApplication.properties" "football/DistanceAndSpeedAnalysisApplication.properties" "football/PassCombinationDetectionApplication.properties" "football/PressingAnalysisApplication.properties" "football/TeamAreaApplication.properties")
for workerConfigFile in ${workerConfigFiles[@]}; do
  ./runHDFSFileUploader.sh $MASTER:8020 deploy/config/$workerConfigFile input/config/$workerConfigFile
  ./deploy/bin/run-app.sh --streamTeam-config-path=deploy/config/StreamTeam.properties --worker-config-path=deploy/config/$workerConfigFile
done