﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using DotNetCore.CAP.Custom.Internal;
using DotNetCore.CAP.Diagnostics;
using DotNetCore.CAP.Infrastructure;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Models;
using DotNetCore.CAP.Processor;
using DotNetCore.CAP.Processor.States;
using Microsoft.Extensions.Logging;

namespace DotNetCore.CAP.Custom
{
    internal class CustomDefaultSubscriberExecutor : ISubscriberExecutor
    {
        private readonly CAP.Internal.ICallbackMessageSender _callbackMessageSender;

        private readonly IStorageConnection _connection;
        private readonly ILogger _logger;

        public IConsumerInvoker Invoker { get; }

        private readonly IStateChanger _stateChanger;
        private readonly CapOptions _options;
        private readonly CustomMethodMatcherCache _selector;
        private static readonly DiagnosticListener s_diagnosticListener =
            new DiagnosticListener(CapDiagnosticListenerExtensions.DiagnosticListenerName);
        public CustomDefaultSubscriberExecutor(
            ILogger<DefaultSubscriberExecutor> logger,
            CapOptions options,
            IConsumerInvokerFactory consumerInvokerFactory,
            ICallbackMessageSender callbackMessageSender,
            IStateChanger stateChanger,
            IStorageConnection connection,
            CustomMethodMatcherCache selector)
        {
            _selector = selector;
            _callbackMessageSender = callbackMessageSender;
            _options = options;
            _stateChanger = stateChanger;
            _connection = connection;
            _logger = logger;

            Invoker = consumerInvokerFactory.CreateInvoker();
        }
        public async Task<OperateResult> ExecuteAsync(CapReceivedMessage message)
        {
            bool retry;
            OperateResult result;
            do
            {
                var executedResult = await ExecuteWithOutRetryAsync(message);
                result = executedResult.Item2;
                if (result == OperateResult.Success)
                {
                    return result;
                }
                retry = executedResult.Item1;
            } while (retry);

            return result;
        }

        /// <summary>
        /// Execute message consumption once.
        /// </summary>
        /// <param name="message">the message received of<see cref="CapReceivedMessage"/></param>
        /// <returns>Item1 is need still retry, Item2 is executed result</returns>
        private async Task<(bool,OperateResult)> ExecuteWithOutRetryAsync(CapReceivedMessage message)
        {
            if(message ==null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            try
            {
                var sp = Stopwatch.StartNew();

                await InvokeConsumerMethodAsync(message);

                sp.Stop();

                await SetSuccessfulState(message);

                _logger.ConsumerExecuted(sp.Elapsed.TotalMilliseconds);

                return (false, OperateResult.Success);
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, $"An exception occurred while executing the subscription method. Topic:{message.Name},Id:{message.Id}");

                return (await SetFailedState(message, ex), OperateResult.Failed(ex));
            }
        }

        private async Task<bool> SetFailedState(CapReceivedMessage message, Exception ex)
        {
            if(ex is SubscriberNotFoundException)
            {
                message.Retries = _options.FailedRetryCount; // not retry if SubscriberNotFoundException
            }

            AddErrorReasonToContent(message, ex);

            var needRetry = UpdateMessageForRetry(message);

            await _stateChanger.ChangeStateAsync(message, new FailedState(), _connection);

            return needRetry;
        }

        private bool UpdateMessageForRetry(CapReceivedMessage message)
        {
            var retryBehavior = RetryBehavior.DefaultRetry;

            var retries = ++message.Retries;
            message.ExpiresAt = message.Added.AddSeconds(retryBehavior.RetryIn(retries));

            var retryCount = Math.Min(_options.FailedRetryCount, retryBehavior.RetryCount);
            if(retries >= retryCount)
            {
                if(retries == _options.FailedRetryCount)
                {
                    try
                    {
                        _options.FailedThresholdCallback?.Invoke(MessageType.Subscribe, message.Name, message.Content);

                        _logger.ConsumerExecutedAfterThreshold(message.Id, _options.FailedRetryCount);
                    }
                    catch(Exception ex)
                    {
                        _logger.ExecutedThresholdCallbackFailed(ex);
                    }

                }
                return false;
            }

            _logger.ConsumerExecutionRetrying(message.Id, retries);

            return true;
           
        }

        private void AddErrorReasonToContent(CapReceivedMessage message, Exception ex)
        {
            message.Content = Helper.AddExceptionProperty(message.Content, ex);
        }

        private Task SetSuccessfulState(CapReceivedMessage message)
        {
            var succeededState = new SucceededState(_options.SucceedMessageExpiredAfter);
            return _stateChanger.ChangeStateAsync(message, succeededState, _connection);
        }

        private async Task InvokeConsumerMethodAsync(CapReceivedMessage receivedMessage)
        {
            if(!_selector.TryGetTopicExecutor(receivedMessage.Name, receivedMessage.Group,
                out var executor))
            {
                var error = $"Message can not be found subscriber,{receivedMessage} \r\n see:https://github.com/dotnetcore/CAP/issues/63";
                throw new SubscriberNotFoundException(error);
            }

            var startTime = DateTimeOffset.UtcNow;
            var stopwatch = Stopwatch.StartNew();
            var operationId = Guid.Empty;

            var consumerContext = new ConsumerContext(executor, receivedMessage.ToMessageContext());

            try
            {
                operationId = s_diagnosticListener.WriteSubscriberInvokeBefore(consumerContext);

                var ret = await Invoker.InvokeAsync(consumerContext);

                s_diagnosticListener.WriteSubscriberInvokeAfter(operationId, consumerContext, startTime, stopwatch.Elapsed);

                if(!string.IsNullOrEmpty(ret.CallbackName))
                {
                    await _callbackMessageSender.SendAsync(ret.MessageId, ret.CallbackName, ret.Result);
                }
            }
            catch(Exception ex)
            {
                s_diagnosticListener.WriteSubscriberInvokeError(operationId, consumerContext, ex, startTime, stopwatch.Elapsed);
            }
        }
    }
}
