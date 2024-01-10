/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssql.initialsync;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.integrations.source.relationaldb.models.CdcState;
import io.airbyte.cdk.integrations.source.relationaldb.models.DbStreamState;
import io.airbyte.cdk.integrations.source.relationaldb.models.OrderedColumnLoadStatus;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.mssql.initialsync.MssqlInitialReadUtil.InitialLoadStreams;
import io.airbyte.integrations.source.mssql.initialsync.MssqlInitialReadUtil.OrderedColumnInfo;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.v0.AirbyteGlobalState;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.v0.AirbyteStreamState;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import io.airbyte.protocol.models.v0.SyncMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MssqlInitialLoadGlobalStateManager implements MssqlInitialLoadStateManager {

  private final Map<AirbyteStreamNameNamespacePair, OrderedColumnLoadStatus> pairToOrderedColLoadStatus;
  private final Map<AirbyteStreamNameNamespacePair, OrderedColumnInfo> pairToOrderedColInfo;
  private final CdcState cdcState;

  // Only one global state is emitted, which is fanned out into many entries in the DB by platform. As
  // a result, we need to keep track of streams that
  // have completed the snapshot.
  private final Set<AirbyteStreamNameNamespacePair> streamsThatHaveCompletedSnapshot;

  public MssqlInitialLoadGlobalStateManager(final InitialLoadStreams initialLoadStreams,
                                            final Map<AirbyteStreamNameNamespacePair, OrderedColumnInfo> pairToOrderedColInfo,
                                            final CdcState cdcState,
                                            final ConfiguredAirbyteCatalog catalog) {
    this.cdcState = cdcState;
    this.pairToOrderedColLoadStatus = MssqlInitialLoadStateManager.initPairToOrderedColumnLoadStatusMap(initialLoadStreams.pairToInitialLoadStatus());
    this.pairToOrderedColInfo = pairToOrderedColInfo;
    this.streamsThatHaveCompletedSnapshot = initStreamsCompletedSnapshot(initialLoadStreams, catalog);
  }

  private static Set<AirbyteStreamNameNamespacePair> initStreamsCompletedSnapshot(final InitialLoadStreams initialLoadStreams,
                                                                                  final ConfiguredAirbyteCatalog catalog) {
    final Set<AirbyteStreamNameNamespacePair> streamsThatHaveCompletedSnapshot = new HashSet<>();
    catalog.getStreams().forEach(configuredAirbyteStream -> {
      if (!initialLoadStreams.streamsForInitialLoad().contains(configuredAirbyteStream)
          && configuredAirbyteStream.getSyncMode() == SyncMode.INCREMENTAL) {
        streamsThatHaveCompletedSnapshot.add(
            new AirbyteStreamNameNamespacePair(configuredAirbyteStream.getStream().getName(), configuredAirbyteStream.getStream().getNamespace()));
      }
    });
    return streamsThatHaveCompletedSnapshot;
  }

  @Override
  public AirbyteStateMessage createIntermediateStateMessage(final AirbyteStreamNameNamespacePair pair, final OrderedColumnLoadStatus ocLoadStatus) {
    final List<AirbyteStreamState> streamStates = new ArrayList<>();
    streamsThatHaveCompletedSnapshot.forEach(stream -> {
      final DbStreamState state = getFinalState(stream);
      streamStates.add(getAirbyteStreamState(stream, Jsons.jsonNode(state)));
    });
    streamStates.add(getAirbyteStreamState(pair, (Jsons.jsonNode(ocLoadStatus))));
    final AirbyteGlobalState globalState = new AirbyteGlobalState();
    globalState.setSharedState(Jsons.jsonNode(cdcState));
    globalState.setStreamStates(streamStates);

    return new AirbyteStateMessage()
        .withType(AirbyteStateType.GLOBAL)
        .withGlobal(globalState);
  }

  private AirbyteStreamState getAirbyteStreamState(final io.airbyte.protocol.models.AirbyteStreamNameNamespacePair pair, final JsonNode stateData) {
    assert Objects.nonNull(pair);
    assert Objects.nonNull(pair.getName());
    assert Objects.nonNull(pair.getNamespace());

    return new AirbyteStreamState()
        .withStreamDescriptor(
            new StreamDescriptor().withName(pair.getName()).withNamespace(pair.getNamespace()))
        .withStreamState(stateData);
  }

  @Override
  public void updateOrderedColumnLoadState(final AirbyteStreamNameNamespacePair pair, final OrderedColumnLoadStatus ocLoadStatus) {
    pairToOrderedColLoadStatus.put(pair, ocLoadStatus);
  }

  @Override
  public AirbyteStateMessage createFinalStateMessage(final AirbyteStreamNameNamespacePair pair, final JsonNode streamStateForIncrementalRun) {
    streamsThatHaveCompletedSnapshot.add(pair);
    final List<AirbyteStreamState> streamStates = new ArrayList<>();
    streamsThatHaveCompletedSnapshot.forEach(stream -> {
      final DbStreamState state = getFinalState(stream);
      streamStates.add(getAirbyteStreamState(stream, Jsons.jsonNode(state)));
    });

    final AirbyteGlobalState globalState = new AirbyteGlobalState();
    globalState.setSharedState(Jsons.jsonNode(cdcState));
    globalState.setStreamStates(streamStates);

    return new AirbyteStateMessage()
        .withType(AirbyteStateType.GLOBAL)
        .withGlobal(globalState);
  }

  @Override
  public OrderedColumnLoadStatus getOrderedColumnLoadStatus(final AirbyteStreamNameNamespacePair pair) {
    return pairToOrderedColLoadStatus.get(pair);
  }

  @Override
  public OrderedColumnInfo getOrderedColumnInfo(final AirbyteStreamNameNamespacePair pair) {
    return pairToOrderedColInfo.get(pair);
  }

  private DbStreamState getFinalState(final io.airbyte.protocol.models.AirbyteStreamNameNamespacePair pair) {
    assert Objects.nonNull(pair);
    assert Objects.nonNull(pair.getName());
    assert Objects.nonNull(pair.getNamespace());

    return new DbStreamState()
        .withStreamName(pair.getName())
        .withStreamNamespace(pair.getNamespace())
        .withCursorField(Collections.emptyList())
        .withCursor(null);
  }

}
