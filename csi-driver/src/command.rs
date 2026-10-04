//! Commands whose nonzero exit status is always an error.
use std::process::Output;

use tokio::process::Command;
use tonic::Status;

pub(crate) async fn run(program: &str, args: &[&str]) -> Result<Output, Status> {
    let output = Command::new(program)
        .args(args)
        .env("LC_ALL", "C")
        .output()
        .await
        .map_err(|e| Status::internal(format!("{program}: {e}")))?;
    if !output.status.success() {
        return Err(Status::internal(format!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output)
}

pub(crate) async fn text(program: &str, args: &[&str]) -> Result<String, Status> {
    String::from_utf8(run(program, args).await?.stdout)
        .map_err(|_| Status::internal(format!("{program} returned invalid UTF-8")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_contract() {
        assert_eq!(
            text(
                "/bin/sh",
                &["-c", "printf '%s:%s' \"$LC_ALL\" \"$1\"", "sh", "a b;$x"]
            )
            .await
            .unwrap(),
            "C:a b;$x"
        );
        let error = run("/bin/sh", &["-c", "printf 'failure\\n' >&2; exit 1"])
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(error.message(), "/bin/sh failed: failure");
        assert!(
            run("/nonexistent/freebsd-csi-command-test", &[])
                .await
                .is_err()
        );

        // Commands that discard stdout do not require UTF-8 output.
        let args = ["-c", "printf '\\377'"];
        assert_eq!(run("/bin/sh", &args).await.unwrap().stdout, [0xff]);
        assert_eq!(
            text("/bin/sh", &args).await.unwrap_err().message(),
            "/bin/sh returned invalid UTF-8"
        );
    }
}
