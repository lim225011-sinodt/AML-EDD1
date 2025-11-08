#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git推送脚本
支持推送到GitHub和Gitee
"""

import subprocess
import sys

def run_command(cmd, description):
    """运行命令并显示结果"""
    print(f"\n{description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        if result.returncode == 0:
            print(f"{description} 成功")
            if result.stdout.strip():
                print(f"输出: {result.stdout.strip()}")
        else:
            print(f"{description} 失败")
            print(f"错误: {result.stderr.strip()}")
        return result.returncode == 0
    except Exception as e:
        print(f"{description} 异常: {e}")
        return False

def main():
    """主函数"""
    print("开始推送代码到远程仓库...")

    # 检查状态
    run_command("git status", "检查Git状态")

    # 尝试推送到GitHub
    print("\n尝试推送到GitHub...")
    github_success = run_command(
        "git push origin main",
        "推送到GitHub"
    )

    # 尝试推送到Gitee
    print("\n尝试推送到Gitee...")
    gitee_success = run_command(
        "git push gitee main",
        "推送到Gitee"
    )

    # 结果总结
    print("\n" + "="*50)
    print("推送结果总结:")
    print(f"GitHub: {'成功' if github_success else '失败'}")
    print(f"Gitee:  {'成功' if gitee_success else '失败'}")

    if github_success and gitee_success:
        print("\n所有仓库推送成功！")
        return 0
    elif github_success or gitee_success:
        print("\n部分仓库推送成功")
        return 1
    else:
        print("\n所有仓库推送失败")
        print("\n建议检查:")
        print("1. GitHub Personal Access Token是否有效且包含repo权限")
        print("2. Gitee仓库是否存在且有访问权限")
        print("3. 网络连接是否正常")
        return 2

if __name__ == "__main__":
    sys.exit(main())