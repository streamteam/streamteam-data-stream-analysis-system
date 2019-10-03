/*
 * StreamTeam
 * Copyright (C) 2019  University of Basel
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ch.unibas.dmi.dbis.streamTeam;

import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.Geometry;
import ch.unibas.dmi.dbis.streamTeam.dataStructures.ObjectInfo;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.HistoryStore;
import ch.unibas.dmi.dbis.streamTeam.modules.generic.helper.SingleValueStore;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Helper class for the JUnit tests.
 */
public class JUnitTestHelper {

    /**
     * Delta for testing the equality of floating point numbers
     */
    public static double DOUBLE_DELTA = 0.01;

    /**
     * Asserts if a data stream element is as expected.
     *
     * @param dataStreamElement         Data stream element
     * @param expectedDataStreamElement Expected data stream element
     */
    public static void assertDataStreamElement(AbstractImmutableDataStreamElement dataStreamElement, AbstractImmutableDataStreamElement expectedDataStreamElement) throws AbstractImmutableDataStreamElement.CannotRetrieveInformationException {
        assertEquals(expectedDataStreamElement.getStreamName(), dataStreamElement.getStreamName());
        assertEquals(expectedDataStreamElement.getKey(), dataStreamElement.getKey());

        assertEquals(expectedDataStreamElement.isAtomic(), dataStreamElement.isAtomic());
        if (!dataStreamElement.isAtomic()) {
            assertEquals(expectedDataStreamElement.getPhase(), dataStreamElement.getPhase());
        }

        assertEquals(dataStreamElement.getGenerationTimestamp(), expectedDataStreamElement.getGenerationTimestamp());

        assertList(expectedDataStreamElement.getObjectIdentifiersList(), dataStreamElement.getObjectIdentifiersList());
        assertList(expectedDataStreamElement.getGroupIdentifiersList(), dataStreamElement.getGroupIdentifiersList());
        assertList(expectedDataStreamElement.getPositionsList(), dataStreamElement.getPositionsList());

        assertEquals(expectedDataStreamElement.toString(), dataStreamElement.toString()); // Covers payload
    }

    /**
     * Asserts that the history store has a given content for a given data stream element key and a given inner key.
     *
     * @param historyStore           HistoryStore
     * @param dataStreamElementKey   Data stream element key
     * @param innerKey               Inner key
     * @param expectedHistoryContent Expected history store content
     */
    public static void assertHistoryStoreContent(HistoryStore historyStore, String dataStreamElementKey, String innerKey, List expectedHistoryContent) {
        List list = historyStore.getList(dataStreamElementKey, innerKey);
        assertList(expectedHistoryContent, list);
    }

    /**
     * Asserts that the latest element in the history store for a given data stream element key and a given inner key is a given element.
     *
     * @param historyStore         HistoryStore
     * @param dataStreamElementKey Data stream element key
     * @param innerKey             Inner key
     * @param expectedElement      Expected latest element in history store
     */
    public static void assertLatestHistoryStoreElement(HistoryStore historyStore, String dataStreamElementKey, String innerKey, Object expectedElement) {
        Object latestElement = historyStore.getLatest(dataStreamElementKey, innerKey);
        if (expectedElement instanceof Double) {
            if (latestElement instanceof Double) {
                assertEquals((Double) expectedElement, (Double) latestElement, DOUBLE_DELTA);
            } else {
                fail(constructWrongFormatExceptionFailMessage("Latest history store element", "Double"));
            }
        } else if (expectedElement instanceof Geometry.Vector) {
            if (latestElement instanceof Geometry.Vector) {
                assertEquals(((Geometry.Vector) expectedElement).x, ((Geometry.Vector) latestElement).x, DOUBLE_DELTA);
                assertEquals(((Geometry.Vector) expectedElement).y, ((Geometry.Vector) latestElement).y, DOUBLE_DELTA);
                assertEquals(((Geometry.Vector) expectedElement).z, ((Geometry.Vector) latestElement).z, DOUBLE_DELTA);
            } else {
                fail(constructWrongFormatExceptionFailMessage("Latest history store element", "Geometry.Vector"));
            }
        } else {
            assertEquals(expectedElement, latestElement);
        }
    }

    /**
     * Asserts that the history store is empty for a given data stream element key and a given inner key.
     *
     * @param historyStore         HistoryStore
     * @param dataStreamElementKey Data stream element key
     * @param innerKey             Inner key
     */
    public static void assertEmptyHistoryStore(HistoryStore historyStore, String dataStreamElementKey, String innerKey) {
        List list = historyStore.getList(dataStreamElementKey, innerKey);
        if (list != null) {
            fail("Expected historyStore to be empty.");
        }
    }

    /**
     * Asserts that the element in the single value store for a given data stream element key and a given inner key is a given element.
     *
     * @param singleValueStore     SingleValueStore
     * @param dataStreamElementKey Data stream element key
     * @param innerKey             Inner key
     * @param expectedElement      Expected single value store content
     */
    public static void assertSingleValueStoreElement(SingleValueStore singleValueStore, String dataStreamElementKey, String innerKey, Object expectedElement) {
        Object element = singleValueStore.get(dataStreamElementKey, innerKey);
        if (expectedElement instanceof Double) {
            if (element instanceof Double) {
                assertEquals((Double) expectedElement, (Double) element, DOUBLE_DELTA);
            } else {
                fail(constructWrongFormatExceptionFailMessage("Single value store element", "Double"));
            }
        } else if (expectedElement instanceof Geometry.Vector) {
            if (element instanceof Geometry.Vector) {
                assertEquals(((Geometry.Vector) expectedElement).x, ((Geometry.Vector) element).x, DOUBLE_DELTA);
                assertEquals(((Geometry.Vector) expectedElement).y, ((Geometry.Vector) element).y, DOUBLE_DELTA);
                assertEquals(((Geometry.Vector) expectedElement).z, ((Geometry.Vector) element).z, DOUBLE_DELTA);
            } else {
                fail(constructWrongFormatExceptionFailMessage("Single value store element", "Geometry.Vector"));
            }
        } else {
            assertEquals(expectedElement, element);
        }
    }

    /**
     * Asserts that the single value store is empty for a given data stream element key and a given inner key.
     *
     * @param singleValueStore     SingleValueStore
     * @param dataStreamElementKey Data stream element key
     * @param innerKey             Inner key
     */
    public static void assertEmptySingleValueStore(SingleValueStore singleValueStore, String dataStreamElementKey, String innerKey) {
        Object o = singleValueStore.get(dataStreamElementKey, innerKey);
        if (o != null) {
            fail("Expected singleValueStore to be empty.");
        }
    }

    /**
     * Asserts if an ObjectInfo is as expected.
     *
     * @param objectInfo               ObjectInfo
     * @param expectedObjectId         Expected object identifier
     * @param expectedGroupId          Expected group identifier
     * @param expectedPosition         Expected position
     * @param expectedVelocity         Expected velocity
     * @param expectedAbsoluteVelocity Expected absolute velocity
     */
    public static void assertObjectInfo(ObjectInfo objectInfo, String expectedObjectId, String expectedGroupId, Geometry.Vector expectedPosition, Geometry.Vector expectedVelocity, Double expectedAbsoluteVelocity) {
        assertEquals(expectedObjectId, objectInfo.getObjectId());
        assertEquals(expectedGroupId, objectInfo.getGroupId());
        if (expectedPosition == null && objectInfo.getPosition() != null) {
            fail("Expected position to be null but is " + objectInfo.getPosition().toString() + ".");
        } else if (expectedPosition != null && objectInfo.getPosition() == null) {
            fail("Expected position to be " + expectedPosition.toString() + " but is null.");
        } else if (expectedPosition != null && objectInfo.getPosition() != null) {
            assertEquals(expectedPosition.x, objectInfo.getPosition().x, DOUBLE_DELTA);
            assertEquals(expectedPosition.y, objectInfo.getPosition().y, DOUBLE_DELTA);
            assertEquals(expectedPosition.z, objectInfo.getPosition().z, DOUBLE_DELTA);
        }
        if (expectedVelocity == null && objectInfo.getVelocity() != null) {
            fail("Expected velocity to be null but is " + objectInfo.getVelocity().toString() + ".");
        } else if (expectedVelocity != null && objectInfo.getVelocity() == null) {
            fail("Expected velocity to be " + expectedVelocity.toString() + " but is null.");
        } else if (expectedVelocity != null && objectInfo.getVelocity() != null) {
            assertEquals(expectedVelocity.x, objectInfo.getVelocity().x, DOUBLE_DELTA);
            assertEquals(expectedVelocity.y, objectInfo.getVelocity().y, DOUBLE_DELTA);
            assertEquals(expectedVelocity.z, objectInfo.getVelocity().z, DOUBLE_DELTA);
        }
        if (expectedAbsoluteVelocity == null && objectInfo.getAbsoluteVelocity() != null) {
            fail("Expected absolute velocity to be null but is " + objectInfo.getAbsoluteVelocity().toString() + ".");
        } else if (expectedAbsoluteVelocity != null && objectInfo.getAbsoluteVelocity() == null) {
            fail("Expected absolute velocity to be " + expectedAbsoluteVelocity.toString() + " but is null.");
        } else if (expectedAbsoluteVelocity != null && objectInfo.getAbsoluteVelocity() != null) {
            assertEquals(expectedAbsoluteVelocity, objectInfo.getAbsoluteVelocity(), DOUBLE_DELTA);
        }

    }

    /**
     * Asserts if a list is filled as expected.
     *
     * @param expectedList Expected list
     * @param actualList   Actual list
     */
    public static <T> void assertList(List<T> expectedList, List<T> actualList) {
        if (expectedList == null && actualList == null) {
            return; // success
        } else if (expectedList != null && actualList == null) {
            fail("Expected list to be not null but is null.");
        } else if (expectedList == null && actualList != null) {
            fail("Expected list to be null but is not null.");
        } else if (expectedList.size() != actualList.size()) {
            fail(constructWrongNumberFailMessage("list items", expectedList.size(), actualList.size()));
        } else {
            for (int i = 0; i < expectedList.size(); ++i) {

                if (expectedList.get(i) instanceof Double) {
                    if (actualList.get(i) instanceof Double) {
                        assertEquals((Double) expectedList.get(i), (Double) actualList.get(i), DOUBLE_DELTA);
                    } else {
                        fail(constructWrongFormatExceptionFailMessage("List element", "Double"));
                    }
                } else if (expectedList.get(i) instanceof Geometry.Vector) {
                    if (actualList.get(i) instanceof Geometry.Vector) {
                        assertEquals(((Geometry.Vector) expectedList.get(i)).x, ((Geometry.Vector) actualList.get(i)).x, DOUBLE_DELTA);
                        assertEquals(((Geometry.Vector) expectedList.get(i)).y, ((Geometry.Vector) actualList.get(i)).y, DOUBLE_DELTA);
                        assertEquals(((Geometry.Vector) expectedList.get(i)).z, ((Geometry.Vector) actualList.get(i)).z, DOUBLE_DELTA);
                    } else {
                        fail(constructWrongFormatExceptionFailMessage("List element", "Geometry.Vector"));
                    }
                } else {
                    assertEquals(expectedList.get(i), actualList.get(i));
                }
            }
        }
    }

    /**
     * Constructs a fail message for wrong format.
     *
     * @param variableName   Name of the variable
     * @param expectedFormat Expected format
     * @return Fail message
     */
    private static String constructWrongFormatExceptionFailMessage(String variableName, String expectedFormat) {
        StringBuilder res = new StringBuilder(variableName);
        res.append(" has to be a ");
        res.append(expectedFormat);
        res.append(".");
        return res.toString();
    }

    /**
     * Constructs a fail message for wrong number of list elements.
     *
     * @param name     Name of the list which has the wrong number of elements
     * @param expected Expected number
     * @param actual   Actual number
     * @return Fail message
     */
    private static String constructWrongNumberFailMessage(String name, int expected, int actual) {
        StringBuilder res = new StringBuilder("Wrong number of ");
        res.append(name);
        res.append(": expected:<");
        res.append(expected);
        res.append("> but was:<");
        res.append(actual);
        res.append(">");
        return res.toString();
    }
}
