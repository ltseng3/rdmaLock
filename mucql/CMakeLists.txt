cmake_minimum_required(VERSION 3.14)
project(mucql_reference LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_library(mucql_core STATIC src/core.cpp src/paxos.cpp src/tcp_transport.cpp)
target_include_directories(mucql_core PUBLIC include)
target_compile_options(mucql_core PRIVATE -Wall -Wextra -Wpedantic)
find_package(Threads REQUIRED)
add_executable(mucql_test tests/test_core.cpp)
target_link_libraries(mucql_test PRIVATE mucql_core Threads::Threads)
target_compile_options(mucql_test PRIVATE -UNDEBUG)
enable_testing()
add_test(NAME mucql_protocol COMMAND mucql_test)

add_executable(mucql_paxos_test tests/test_paxos.cpp)
target_link_libraries(mucql_paxos_test PRIVATE mucql_core Threads::Threads)
target_compile_options(mucql_paxos_test PRIVATE -UNDEBUG)
add_test(NAME mucql_quorum COMMAND mucql_paxos_test)
add_executable(mucql_batch_test tests/test_batch.cpp)
target_link_libraries(mucql_batch_test PRIVATE mucql_core Threads::Threads)
target_compile_options(mucql_batch_test PRIVATE -UNDEBUG)
add_test(NAME mucql_batch_recovery COMMAND mucql_batch_test)

add_executable(mucql_multi_key_pipeline_test tests/test_multi_key_pipeline.cpp)
target_link_libraries(mucql_multi_key_pipeline_test PRIVATE mucql_core Threads::Threads)
target_compile_options(mucql_multi_key_pipeline_test PRIVATE -UNDEBUG -Wall -Wextra -Wpedantic -Werror)
add_test(NAME mucql_multi_key_phase2_quorum COMMAND mucql_multi_key_pipeline_test)
add_executable(mucql_async_pipeline_test tests/test_async_pipeline.cpp)
target_link_libraries(mucql_async_pipeline_test PRIVATE mucql_core Threads::Threads)
target_compile_options(mucql_async_pipeline_test PRIVATE -UNDEBUG -Wall -Wextra -Wpedantic -Werror)
add_test(NAME mucql_async_phase2_pipeline COMMAND mucql_async_pipeline_test)


add_executable(mucql_online_core_test tests/test_online_core.cpp)
target_link_libraries(mucql_online_core_test PRIVATE mucql_core Threads::Threads)
target_compile_options(mucql_online_core_test PRIVATE -UNDEBUG -Wall -Wextra -Wpedantic -Werror)
add_test(NAME mucql_online_progress_recovery COMMAND mucql_online_core_test)

add_executable(mucql_fault_test tests/test_fault_schedule.cpp)
target_link_libraries(mucql_fault_test PRIVATE mucql_core Threads::Threads)
target_compile_options(mucql_fault_test PRIVATE -UNDEBUG)
add_test(NAME mucql_fault_schedules COMMAND mucql_fault_test)

add_executable(mucql_acceptor src/tcp_acceptor_main.cpp)
target_link_libraries(mucql_acceptor PRIVATE mucql_core Threads::Threads)
add_executable(mucql_paxos_demo src/tcp_client_demo.cpp)
target_link_libraries(mucql_paxos_demo PRIVATE mucql_core Threads::Threads)

# Native libibverbs is optional: the protocol simulator and TCP quorum still
# compile on development machines without an RNIC.
find_path(IBVERBS_INCLUDE_DIR infiniband/verbs.h)
find_library(IBVERBS_LIB ibverbs)
if(IBVERBS_INCLUDE_DIR AND IBVERBS_LIB)
  add_library(mucql_verbs STATIC src/verbs_release.cpp src/verbs_paxos.cpp src/live_control.cpp)
  target_include_directories(mucql_verbs PUBLIC include PRIVATE ${IBVERBS_INCLUDE_DIR})
  target_link_libraries(mucql_verbs PUBLIC mucql_core ${IBVERBS_LIB} Threads::Threads)
  target_compile_options(mucql_verbs PRIVATE -Wall -Wextra -Wpedantic)
  add_executable(mucql_verbs_release src/verbs_release_main.cpp)
  target_link_libraries(mucql_verbs_release PRIVATE mucql_verbs)
  add_executable(mucql_lock_live src/lock_live_main.cpp)
  target_link_libraries(mucql_lock_live PRIVATE mucql_verbs mucql_core Threads::Threads)
  target_compile_options(mucql_lock_live PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_executable(mucql_lock_e2e src/lock_e2e_main.cpp)
  target_link_libraries(mucql_lock_e2e PRIVATE mucql_verbs mucql_core Threads::Threads)
  target_compile_options(mucql_lock_e2e PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_executable(mucql_rdma_paxos src/verbs_paxos_main.cpp)
  target_link_libraries(mucql_rdma_paxos PRIVATE mucql_verbs mucql_core)
else()
  message(STATUS "libibverbs not found: skipping native RDMA release-marker targets")
endif()

# Compile and exercise the SAME verbs source against a local fake RNIC.
# Tests only state/serialization/control bootstrap; never validates real NIC behavior.
option(MUCQL_MOCK_VERBS_TEST "Build mock RDMA transport test" ON)
if(MUCQL_MOCK_VERBS_TEST)
  add_library(mucql_mock_verbs STATIC src/verbs_release.cpp src/live_control.cpp tests/mock_verbs.cpp)
  target_include_directories(mucql_mock_verbs PUBLIC include PRIVATE tests/stub)
  target_link_libraries(mucql_mock_verbs PUBLIC Threads::Threads)
  target_compile_options(mucql_mock_verbs PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_executable(mucql_release_mock_test tests/test_verbs_release.cpp)
  target_link_libraries(mucql_release_mock_test PRIVATE mucql_mock_verbs)
  add_test(NAME mucql_rdma_mock COMMAND mucql_release_mock_test)
endif()

if(MUCQL_MOCK_VERBS_TEST)
  target_sources(mucql_mock_verbs PRIVATE src/verbs_paxos.cpp)
  target_link_libraries(mucql_mock_verbs PUBLIC mucql_core)
  add_executable(mucql_live_mock_test tests/test_live_mock.cpp)
  target_link_libraries(mucql_live_mock_test PRIVATE mucql_mock_verbs mucql_core Threads::Threads)
  target_compile_options(mucql_live_mock_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_test(NAME mucql_live_hot_raw COMMAND mucql_live_mock_test 3 1 1 1 1 2 0 1)
  add_test(NAME mucql_live_hot_optimized COMMAND mucql_live_mock_test 4 1 8 8 8 16 0 1)
  add_test(NAME mucql_live_multilock COMMAND mucql_live_mock_test 4 8 8 8 4 16 1 1)
  add_test(NAME mucql_live_uninstrumented COMMAND mucql_live_mock_test 3 4 4 4 4 8 0 0)
  add_test(NAME mucql_live_16clients_hot_mock COMMAND mucql_live_mock_test 16 1 16 16 16 64 0 1)
  add_test(NAME mucql_live_16clients_zipf_mock COMMAND mucql_live_mock_test 16 64 16 16 16 64 1 1)
  set_tests_properties(mucql_live_16clients_hot_mock mucql_live_16clients_zipf_mock
      PROPERTIES TIMEOUT 35 RUN_SERIAL TRUE)
  set_tests_properties(mucql_live_hot_raw mucql_live_hot_optimized
      mucql_live_multilock mucql_live_uninstrumented PROPERTIES TIMEOUT 35 RUN_SERIAL TRUE)
  add_executable(mucql_lock_live_mock_build src/lock_live_main.cpp)
  target_link_libraries(mucql_lock_live_mock_build PRIVATE mucql_mock_verbs mucql_core Threads::Threads)
  target_compile_options(mucql_lock_live_mock_build PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_executable(mucql_lock_e2e_mock_build src/lock_e2e_main.cpp)
  target_link_libraries(mucql_lock_e2e_mock_build PRIVATE mucql_mock_verbs mucql_core Threads::Threads)
  target_compile_options(mucql_lock_e2e_mock_build PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_executable(mucql_e2e_mock_test tests/test_e2e_mock.cpp)
  target_link_libraries(mucql_e2e_mock_test PRIVATE mucql_mock_verbs mucql_core Threads::Threads)
  target_compile_options(mucql_e2e_mock_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_test(NAME mucql_full_handoff_mock COMMAND mucql_e2e_mock_test 48 2 1 10)
  add_test(NAME mucql_handoff_3clients_mock COMMAND mucql_e2e_mock_test 81 3 1 0)
  add_test(NAME mucql_handoff_uninstrumented_mock COMMAND mucql_e2e_mock_test 64 4 0 200)
  add_test(NAME mucql_handoff_8clients_mock COMMAND mucql_e2e_mock_test 96 8 1 0)
  add_test(NAME mucql_handoff_16clients_mock COMMAND mucql_e2e_mock_test 128 16 0 0)
  set_tests_properties(mucql_full_handoff_mock mucql_handoff_3clients_mock
      mucql_handoff_uninstrumented_mock mucql_handoff_8clients_mock
      mucql_handoff_16clients_mock PROPERTIES TIMEOUT 45 RUN_SERIAL TRUE)
  add_executable(mucql_paxos_rdma_mock_test tests/test_paxos_rdma.cpp)
  target_link_libraries(mucql_paxos_rdma_mock_test PRIVATE mucql_mock_verbs mucql_core)
  add_test(NAME mucql_rdma_quorum_mock COMMAND mucql_paxos_rdma_mock_test)
  # 3.5s mock MR registration cannot consume the 3s phase-1 handshake.
  # The server must preprovision that MR before reporting readiness.
  add_test(NAME mucql_rdma_quorum_slow_preflight_mock COMMAND mucql_paxos_rdma_mock_test)
  set_tests_properties(mucql_rdma_quorum_slow_preflight_mock
      PROPERTIES ENVIRONMENT "MUCQL_MOCK_ACCEPTED_MR_DELAY_MS=3500"
                 TIMEOUT 25 RUN_SERIAL TRUE)
  # Idle RC QPs must survive a TCP bootstrap socket idle for >3 seconds.
  add_test(NAME mucql_rdma_quorum_idle_mock COMMAND mucql_paxos_rdma_mock_test)
  set_tests_properties(mucql_rdma_quorum_idle_mock
      PROPERTIES ENVIRONMENT "MUCQL_TEST_PAXOS_IDLE_MS=3600"
                 TIMEOUT 25 RUN_SERIAL TRUE)
  add_executable(mucql_paxos_preflight_mock_test tests/test_paxos_preflight.cpp)
  target_link_libraries(mucql_paxos_preflight_mock_test PRIVATE mucql_mock_verbs mucql_core Threads::Threads)
  target_compile_options(mucql_paxos_preflight_mock_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_test(NAME mucql_rdma_preflight_reject_unavailable_mr COMMAND mucql_paxos_preflight_mock_test)
endif()

if(MUCQL_MOCK_VERBS_TEST)
# Dedicated frame, receive-ring wraparound and cancellation regression.
add_executable(mucql_live_control_mock_test tests/test_live_control_mock.cpp)
target_link_libraries(mucql_live_control_mock_test PRIVATE mucql_mock_verbs Threads::Threads)
target_compile_options(mucql_live_control_mock_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
add_test(NAME mucql_live_control_recv_recycle COMMAND mucql_live_control_mock_test)

# Both software-MR variants execute the same live control channel.
add_test(NAME mucql_live_rdma_hot_mock COMMAND mucql_live_mock_test 4 1 8 8 8 16 0 1)
add_test(NAME mucql_live_rdma_16clients_mock COMMAND mucql_live_mock_test 16 1 16 16 16 64 0 1)
set_tests_properties(mucql_live_rdma_hot_mock mucql_live_rdma_16clients_mock
  PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma" TIMEOUT 45 RUN_SERIAL TRUE)
set_tests_properties(mucql_live_hot_raw mucql_live_hot_optimized
  mucql_live_multilock mucql_live_uninstrumented
  mucql_live_16clients_hot_mock mucql_live_16clients_zipf_mock
  PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=tcp")
  # 32 sequential completed 8-byte writes per exclusive critical section,
  # exercising the same RNIC write-count dimension as DecLock exec-time.
  add_test(NAME mucql_live_rdma_many_data_writes_mock
      COMMAND mucql_live_mock_test 16 1 1 1 1 2 0 1)
  add_test(NAME mucql_live_rdma_many_data_writes_skew_mock
      COMMAND mucql_live_mock_test 16 64 8 8 8 64 1 1)
  set_tests_properties(mucql_live_rdma_many_data_writes_mock
      mucql_live_rdma_many_data_writes_skew_mock
      PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma;MUCQL_DATA_OPS=32"
                 TIMEOUT 45 RUN_SERIAL TRUE)
  add_test(NAME mucql_live_rdma_release_bind_retry_mock
      COMMAND mucql_live_mock_test 4 1 1 1 1 2 0 1)
  set_tests_properties(mucql_live_rdma_release_bind_retry_mock
      PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma;MUCQL_TEST_RELEASE_BIND_DELAY_MS=60"
                 TIMEOUT 45 RUN_SERIAL TRUE)
  add_test(NAME mucql_live_rdma_handoff_preconnect_mock
      COMMAND mucql_live_mock_test 16 1 1 1 1 2 0 1)
  set_tests_properties(mucql_live_rdma_handoff_preconnect_mock
      PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma;MUCQL_HANDOFF_PRECONNECT=1"
                 TIMEOUT 45 RUN_SERIAL TRUE)
  add_test(NAME mucql_live_rdma_legacy_handoff_mock
      COMMAND mucql_live_mock_test 4 1 1 1 1 2 0 1)
  set_tests_properties(mucql_live_rdma_legacy_handoff_mock
      PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma;MUCQL_HANDOFF_PRECONNECT=0"
                 TIMEOUT 45 RUN_SERIAL TRUE)
  add_test(NAME mucql_live_rdma_idle_poll_mock
      COMMAND mucql_live_mock_test 16 1 1 1 1 2 0 1)
  set_tests_properties(mucql_live_rdma_idle_poll_mock
      PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma;MUCQL_RX_IDLE_US=5;MUCQL_HANDOFF_IDLE_US=5;MUCQL_HANDOFF_PRECONNECT=1"
                 TIMEOUT 45 RUN_SERIAL TRUE)
  add_test(NAME mucql_live_control_idle_recv_mock COMMAND mucql_live_control_mock_test)
  set_tests_properties(mucql_live_control_idle_recv_mock
      PROPERTIES ENVIRONMENT "MUCQL_RX_IDLE_US=5"
                 TIMEOUT 30 RUN_SERIAL TRUE)

  # Large logical key populations reuse the fixed-sized accepted log budget.
  # Both variants verify disjoint per-key data/owner regions and all release
  # markers using mock RDMA; no log ring reclamation is enabled.
  add_test(NAME mucql_live_rdma_100k_uniform_mock
      COMMAND mucql_live_mock_test 4 100000 1 1 1 2 0 1)
  add_test(NAME mucql_live_rdma_100k_zipf_mock
      COMMAND mucql_live_mock_test 4 100000 1 1 1 2 1 1)
  set_tests_properties(mucql_live_rdma_100k_uniform_mock
      mucql_live_rdma_100k_zipf_mock
      PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma;MUCQL_HANDOFF_PRECONNECT=0"
                 TIMEOUT 45 RUN_SERIAL TRUE)

  # Real cross-lock Phase-2 pipelining: the coordinator keeps accepting
  # events during quorum CQ draining; ticket activation still requires a
  # majority-decided JOIN followed by a majority-decided PREPARE.
  add_test(NAME mucql_live_rdma_multikey_async_mock
      COMMAND mucql_live_mock_test 16 100000 16 16 1 2 0 1)
  add_test(NAME mucql_live_rdma_multikey_async_zipf_mock
      COMMAND mucql_live_mock_test 16 16 16 16 1 2 1 1)
  add_test(NAME mucql_live_rdma_multikey_delayed_mock
      COMMAND mucql_live_mock_test 16 100000 4 16 1 2 0 1)
  add_test(NAME mucql_live_rdma_hotkey_async_mock
      COMMAND mucql_live_mock_test 16 1 4 16 1 2 0 1)
  set_tests_properties(mucql_live_rdma_multikey_async_mock
      mucql_live_rdma_multikey_async_zipf_mock
      PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma;MUCQL_MULTI_KEY_PIPELINE=1;MUCQL_HANDOFF_PRECONNECT=0"
                 TIMEOUT 45 RUN_SERIAL TRUE)
  set_tests_properties(mucql_live_rdma_multikey_delayed_mock
      mucql_live_rdma_hotkey_async_mock
      PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma;MUCQL_MULTI_KEY_PIPELINE=1;MUCQL_HANDOFF_PRECONNECT=0;MUCQL_MOCK_PHASE2_DELAY_POLLS=20"
                 TIMEOUT 45 RUN_SERIAL TRUE)
  set_tests_properties(mucql_live_rdma_multikey_delayed_mock PROPERTIES
      PASS_REGULAR_EXPRESSION "phase2_overlap_submissions=[1-9][0-9]*")

  # The synchronized multi-instance GO gate is opt-in and leaves normal
  # benchmark latency accounting unchanged when unset.
  add_test(NAME mucql_live_rdma_synchronized_go_mock
      COMMAND mucql_live_mock_test 4 16 1 1 1 2 0 1)
  set_tests_properties(mucql_live_rdma_synchronized_go_mock
      PROPERTIES ENVIRONMENT "MUCQL_CN_TRANSPORT=rdma;MUCQL_TEST_START_GATE=1"
                 TIMEOUT 45 RUN_SERIAL TRUE)
endif()
