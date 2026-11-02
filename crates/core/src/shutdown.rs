use std::io;

pub async fn signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

#[cfg(all(test, unix))]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn sigterm_resolves_the_signal() {
        let pid = std::process::id().to_string();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            std::process::Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .expect("run kill");
        });

        let result = tokio::time::timeout(Duration::from_secs(10), signal()).await;

        assert!(matches!(result, Ok(Ok(()))));
    }
}
