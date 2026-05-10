{ lib, ... }:
{
  perSystem =
    { pkgs, self', ... }:
    let
      controlPlane = self'.packages.cmsx-control-plane;
      worker = self'.packages.cmsx-worker;

      serverUrl = "http://127.0.0.1:3000";
      adminToken = "e2e-admin-token";

      # Generated with:
      # SQLX_OFFLINE=true cargo run -q -p cmsx-control-plane -- admin hash-token e2e-admin-token
      adminTokenHash = "$argon2id$v=19$m=19456,t=2,p=1$Hxery+8UIuw+qDOuQAeeUQ$4AqeT7M2jT1Y8uAFGdoHnUvZuIJgIuSm5GEHC8FLpMI";

      toml = pkgs.formats.toml { };

      pythonWithSdk = pkgs.python314.withPackages (_: [
        self'.packages.cmsx-autograder-python
      ]);

      controlPlaneConfig = toml.generate "cmsx-e2e-control-plane.toml" {
        bind_addr = "127.0.0.1:3000";
        database_url = "postgresql://cmsx-e2e@localhost/cmsx-e2e?host=/run/postgresql";

        storage = {
          backend = "local";
          root = "/var/lib/cmsx-control-plane/storage";
          prefix = "e2e";
        };

        admin = {
          public_url = serverUrl;
          bootstrap_token_hashes = [ adminTokenHash ];
        };
      };

      mkE2eCmsxInWorker = pkgs.testers.nixosTest {
        name = "e2e-cmsx-in-worker";

        nodes.machine =
          { pkgs, lib, ... }:
          {
            virtualisation.memorySize = 2048;

            networking.firewall.enable = false;

            environment.systemPackages = [
              pkgs.curl
              pkgs.jq
              pkgs.python314
              controlPlane
              worker
            ];

            users = {
              groups.cmsx-e2e = { };
              users.cmsx-e2e = {
                isSystemUser = true;
                group = "cmsx-e2e";
              };

              groups.cmsx-worker = { };
              users.cmsx-worker = {
                isSystemUser = true;
                group = "cmsx-worker";
              };
            };

            services.postgresql = {
              enable = true;
              ensureDatabases = [ "cmsx-e2e" ];
              ensureUsers = [
                {
                  name = "cmsx-e2e";
                  ensureDBOwnership = true;
                }
              ];
            };

            systemd.services.cmsx-control-plane = {
              description = "CMSX control plane e2e service";
              wantedBy = [ "multi-user.target" ];
              after = [
                "postgresql.service"
                "postgresql-setup.service"
              ];
              requires = [
                "postgresql.service"
                "postgresql-setup.service"
              ];

              environment = {
                CMSX_CONFIG = "${controlPlaneConfig}";
              };

              serviceConfig = {
                ExecStartPre = "${pkgs.coreutils}/bin/mkdir -p /var/lib/cmsx-control-plane/storage";
                ExecStart = "${lib.getExe controlPlane}";
                User = "cmsx-e2e";
                Group = "cmsx-e2e";
                StateDirectory = "cmsx-control-plane";
                Restart = "on-failure";
              };
            };

            systemd.services.cmsx-worker = {
              description = "CMSX worker e2e service";
              after = [ "cmsx-control-plane.service" ];
              requires = [ "cmsx-control-plane.service" ];

              environment = {
                CMSX_WORKER_CONFIG = "/run/cmsx-worker/e2e-worker.toml";
              };

              serviceConfig = {
                ExecStart = "${lib.getExe worker}";
                User = "cmsx-worker";
                Group = "cmsx-worker";
                StateDirectory = "cmsx-worker";
                Restart = "on-failure";
              };
            };
          };

        testScript = ''
          import json
          import time
          import shlex
          import textwrap

          machine.start()
          machine.wait_for_unit("multi-user.target")
          machine.wait_for_unit("postgresql.service")
          machine.wait_for_unit("cmsx-control-plane.service")

          machine.wait_until_succeeds(
              "curl -fsS ${serverUrl}/healthz | grep -qx ok"
          )

          auth_header = "Authorization: Bearer ${adminToken}"
          auth_header_arg = shlex.quote(auth_header)

          machine.succeed(textwrap.dedent("""
            cat > /tmp/create-assignment.json <<'EOF'
            {
              "slug": "hello-python",
              "name": "Hello Python",
              "max_score": 10,
              "execution_config": {
                "timeout_seconds": 30
              },
              "runner_config": {},
              "capabilities": {
                "read_files": true,
                "run_commands": false,
                "execute_student_code": false,
                "network": false
              }
            }
            EOF

            curl -fsS \
              -H %s \
              -H 'Content-Type: application/json' \
              -d @/tmp/create-assignment.json \
              ${serverUrl}/admin/assignments \
              > /tmp/assignment.json
            jq -e '.slug == "hello-python"' /tmp/assignment.json
          """) % auth_header_arg)

          machine.succeed(textwrap.dedent("""
            curl -fsS \
              -H %s \
              -H 'Content-Type: application/json' \
              -d '{}' \
              ${serverUrl}/admin/assignments/hello-python/tokens \
              > /tmp/assignment-token.json
            jq -r '.token' /tmp/assignment-token.json > /tmp/assignment-token
            test -s /tmp/assignment-token
          """) % auth_header_arg)

          machine.succeed(textwrap.dedent("""
            curl -fsS \
              -H %s \
              -H 'Content-Type: application/json' \
              -d '{"name":"e2e-worker"}' \
              ${serverUrl}/admin/workers \
              > /tmp/worker.json
            jq -r '.private_key_base64' /tmp/worker.json > /tmp/worker-private-key
            test -s /tmp/worker-private-key
          """) % auth_header_arg)

          machine.succeed(textwrap.dedent("""
            mkdir -p /run/cmsx-worker

            cat > /run/cmsx-worker/e2e-worker.toml <<EOF
            control_plane_url = "${serverUrl}"
            private_key_base64 = "$(cat /tmp/worker-private-key)"
            version = "0.1.0"

            [executor]
            backend = "in-worker"
            workspace_root = "/var/lib/cmsx-worker/jobs"
            grader_root = "${../examples/assignments}"
            max_jobs = 1
            keep_workspaces = false
            python_command = "${pythonWithSdk}/bin/python3"
            EOF

            chown root:root /run/cmsx-worker/e2e-worker.toml
            chmod 0644 /run/cmsx-worker/e2e-worker.toml
          """))

          machine.start_job("cmsx-worker.service")
          machine.wait_for_unit("cmsx-worker.service")

          machine.wait_until_succeeds(textwrap.dedent("""
            curl -fsS -H %s ${serverUrl}/admin/workers \
              | jq -e '.[] | select(.name == "e2e-worker" and .status == "online")'
          """) % auth_header_arg)

          machine.succeed(textwrap.dedent("""
            mkdir -p /tmp/cmsx-submit
            cat > /tmp/cmsx-submit/hello.py <<'EOF'
            print("hello from submitted file")
            EOF

            curl -fsS \
              -F "auth_token=$(cat /tmp/assignment-token)" \
              -F "netids=alice,bob" \
              -F "group_id=group-1" \
              -F "assignment_id=cmsx-assignment-123" \
              -F "assignment_name=Hello Python From CMSX" \
              -F "num_files=1" \
              -F "problem_name_0=hello" \
              -F "file_name_0=hello.py" \
              -F "hello.py=@/tmp/cmsx-submit/hello.py;filename=hello.py" \
              ${serverUrl}/cmsx/a/hello-python/submit
          """))

          submission_id = None
          job_id = None

          for attempt in range(60):
              body = machine.succeed(
                  f"curl -fsS -H {auth_header_arg} ${serverUrl}/assignments/hello-python/submissions"
              )
              submissions = json.loads(body)

              if submissions:
                  submission = submissions[0]
                  submission_id = submission["id"]

                  latest_job = submission.get("latest_job")
                  if latest_job is not None:
                      job_id = latest_job["id"]
                      break

              time.sleep(1)

          assert submission_id is not None, "submission was not created"
          assert job_id is not None, "grading job was not created"

          terminal_job = None

          for attempt in range(120):
              body = machine.succeed(
                  f"curl -fsS -H {auth_header_arg} ${serverUrl}/jobs/{job_id}"
              )
              job = json.loads(body)

              if job["status"] in ["succeeded", "failed", "error", "cancelled"]:
                  terminal_job = job
                  break

              time.sleep(1)

          assert terminal_job is not None, "job did not reach a terminal status"
          assert terminal_job["status"] == "succeeded", terminal_job
          assert terminal_job["attempts"] == 1
          assert terminal_job["result"] is not None
          assert terminal_job["result"]["status"] == "passed"
          assert terminal_job["result"]["score"] == 10
          assert terminal_job["result"]["max_score"] == 10

          results = json.loads(machine.succeed(
              f"curl -fsS -H {auth_header_arg} ${serverUrl}/submissions/{submission_id}/results"
          ))

          assert len(results) == 1, results
          assert results[0]["job_id"] == job_id
          assert results[0]["job_status"] == "succeeded"
          assert results[0]["result_status"] == "passed"
          assert results[0]["score"] == 10
          assert results[0]["max_score"] == 10
          assert len(results[0]["tests"]) == 1
          assert results[0]["tests"][0]["name"] == "submitted hello.py"
          assert results[0]["tests"][0]["status"] == "passed"

          events_body = machine.succeed(
              f"curl -fsS -H {auth_header_arg} ${serverUrl}/jobs/{job_id}/events"
          )
          events = json.loads(events_body)["events"]

          event_types = [event["type"] for event in events]
          assert "job.input.prepared" in event_types, event_types
          assert "executor.started" in event_types, event_types
          assert "result.read" in event_types, event_types

          machine.succeed(textwrap.dedent("""
            curl -fsS -H %s ${serverUrl}/assignments/hello-python/submissions \
              | jq -e '.[0].cmsx_group_id == "group-1"'
          """) % auth_header_arg)
        '';
      };

      e2eChecks = lib.optionalAttrs pkgs.stdenv.isLinux {
        e2e-cmsx-in-worker = mkE2eCmsxInWorker;
      };
    in
    {
      checks = e2eChecks;
    };
}
