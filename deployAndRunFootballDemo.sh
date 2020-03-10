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

cd $DIR

export HADOOP_CONF_DIR=deploy/hadoopConfig
rm -rf target/
rm -rf deploy
mvn clean package
mkdir -p deploy
tar -xf ./target/streamteam-data-stream-analysis-system-1.0.1-dist.tar.gz -C deploy
./runHDFSFileUploader.sh 10.34.58.65:8020 target/streamteam-data-stream-analysis-system-1.0.1-dist.tar.gz input/streamteam-data-stream-analysis-system-1.0.1-dist.tar.gz
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/FieldObjectStateGenerationTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/KickoffDetectionTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/TimeTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/BallPossessionTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/KickDetectionTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/PassAndShotDetectionTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/OffsideTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/HeatmapTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/AreaDetectionTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/SetPlayDetectionTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/DistanceAndSpeedAnalysisTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/PassCombinationDetectionTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/PressingAnalysisTask.properties
deploy/bin/run-job.sh --config-factory=ch.unibas.dmi.dbis.streamTeam.samzaExtensions.StreamTeamConfigFactory --config-path=file://$PWD/deploy/config/football/TeamAreaTask.properties