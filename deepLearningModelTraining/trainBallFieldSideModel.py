#!/usr/bin/python

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

# TODO: The Ball Field Side Worker is a relatively useless worker that was only introduced to check if StreamTeam can perform Deep Learning based analyses. Remove this file as soon as there is a more meaningful Deep Learning based analysis worker.

import numpy as np
from tensorflow.keras import models, layers

# === CONFIGURATION ===
numSamples = 1024 * 100

numTrainingSamples = 1024 * 80
numValidationSamples = 1024 * 10
numTestSamples = 1024 * 10
assert (numTrainingSamples + numValidationSamples + numTestSamples == numSamples)

numEpochs = 20
batchSize = 512

savePath = '../src/main/resources/models/ballFieldSideModel.h5'

# === GENERATE RANDOM DATA ===
# (Expects that the x and y positions are normalized to [-1.0,1.0) and that the z position is always 0.0)
data = np.random.uniform(-1.0, 1.0, (numSamples, (1 + 11 + 11) * 3))
data[:, 2::3] = 0.0

# === GENERATE THE TARGETS AUTOMATICALLY FROM THE RANDOM DATA ===
# 1.0 = ball on right side of the field, 0.0 = ball on left side of the field
# (Expects that d[0] is the x position of the ball)
targets = [1.0 if d[0] >= 0 else 0.0 for d in data]

# === SPLITS THE DATA AND THE TARGETS INTO A TRAINING, A VALIDATION, AND A TEST SET ===
training_data = np.asarray(data[:numTrainingSamples]).astype('float32')
training_targets = np.asarray(targets[:numTrainingSamples]).astype('float32')

validation_data = np.asarray(data[numTrainingSamples:numTrainingSamples + numValidationSamples]).astype('float32')
validation_targets = np.asarray(targets[numTrainingSamples:numTrainingSamples + numValidationSamples]).astype('float32')

test_data = np.asarray(data[numTrainingSamples + numValidationSamples:]).astype('float32')
test_targets = np.asarray(targets[numTrainingSamples + numValidationSamples:]).astype('float32')

# === BUILD THE MODEL ===
model = models.Sequential()
model.add(layers.Dense(64, activation='relu', input_shape=((1 + 11 + 11) * 3,)))
model.add(layers.Dense(1, activation='sigmoid'))

model.summary()

# === COMPILE THE MODEL ===
model.compile(optimizer='rmsprop', loss='binary_crossentropy', metrics=['accuracy'])

# === TRAIN THE MODEL ===
history = model.fit(training_data, training_targets, epochs=numEpochs, batch_size=batchSize, validation_data=(validation_data, validation_targets))

# === TEST THE MODEL ===
results = model.evaluate(test_data, test_targets)
print('Test accuracy on {} new samples: {}'.format(numTestSamples, results[1]))

# === SAVE THE MODEL ===
model.save(savePath)
print('Saved model to {}'.format(savePath))
